// Package master contains the master process initiating and coordinating the
// whole process of executing a MapReduce job.
package master

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amsibamsi/knausrig/cfg"
	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/msg"
	"github.com/amsibamsi/knausrig/util"
)

var (
	logger = log.New(os.Stderr, "[master] ", log.LstdFlags)
)

// Service holds the RPC service exposed to mappers and reducers. All exported
// methods on this struct are considered for exposing via RPC.
type Service struct {
	m *Master
}

// NewService returns a new service for the given master.
func NewService(m *Master) *Service {
	return &Service{m: m}
}

// Ping tests connectivity.
func (s *Service) Ping(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	return nil
}

// NewReducer registers a reducer with the specified <address>:<port>.
func (s *Service) NewReducer(addr *string, _ *msg.EmptyMsg) error {
	s.m.lock.Lock()
	defer s.m.lock.Unlock()
	rmap := s.m.reducerMap
	p := len(rmap)
	max := len(s.m.config.Reducers)
	if p >= max {
		return errors.New("Trying to register too many reducers")
	}
	s.m.reducerMap[p] = *addr
	s.m.staging <- struct{}{}
	logger.Printf("New reducer %q for partition %d", *addr, p)
	return nil
}

// NewMapper registers a new mapper and sends the reducers list to a mapper
func (s *Service) NewMapper(_ *msg.EmptyMsg, mapInfo *msg.MapperInfo) error {
	s.m.lock.Lock()
	s.m.lock.Unlock()
	max := len(s.m.config.Mappers)
	if s.m.registeredMappers >= max {
		return errors.New("Trying to register too many mappers")
	}
	s.m.registeredMappers++
	p := s.m.registeredMappers - 1
	mapInfo.Partition = p
	mapInfo.Reducers = s.m.reducerMap
	s.m.staging <- struct{}{}
	logger.Printf("New mapper for partition %d", p)
	return nil
}

// MapperFinished indicates that a mapper has finished processing and sent all
// data to reducers.
func (s *Service) MapperFinished(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	s.m.staging <- struct{}{}
	return nil
}

// ReducerFinished indicates that a reducer has finished reducing all its data.
func (s *Service) ReducerFinished(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	s.m.staging <- struct{}{}
	return nil
}

// Output takes output from a reducer. Assuming that there is going to be at
// most one call per key.
func (s *Service) Output(req map[string]string, _ *msg.EmptyMsg) error {
	s.m.lock.Lock()
	defer s.m.lock.Unlock()
	for k, v := range req {
		s.m.output[k] = v
	}
	return nil
}

// Master initializes everything and controls the execution of a MapReduce job.
type Master struct {
	config            *cfg.Config
	service           *Service
	lock              *sync.Mutex
	listener          net.Listener
	rpcServer         *rpc.Server
	reducerMap        map[int]string
	output            map[string]string
	outputFn          mapreduce.OutputFn
	remoteCmds        []*exec.Cmd
	fail              chan struct{}
	staging           chan struct{}
	reducerClients    *util.RPCClients
	registeredMappers int
	tasks             *sync.WaitGroup
	events            chan int
}

// NewMaster ...
func NewMaster(config *cfg.Config, outputFn mapreduce.OutputFn) *Master {
	m := Master{
		config:         config,
		lock:           &sync.Mutex{},
		reducerMap:     make(map[int]string),
		output:         make(map[string]string),
		outputFn:       outputFn,
		remoteCmds:     make([]*exec.Cmd, 0),
		fail:           make(chan struct{}),
		staging:        make(chan struct{}),
		reducerClients: util.NewRPCClients(),
		tasks:          &sync.WaitGroup{},
		events:         make(chan int, 1),
	}
	m.service = NewService(&m)
	for i, r := range config.Reducers {
		m.reducerMap[i] = r
	}
	return &m
}

const (
	// remoteRunTmpl is template to use for running current executable over SSH
	// remotely. Should be expaned with Printf to add arguments for binary
	// executed remotely.
	//
	// Note: Carefully construct shell command in a way that the temp binary is
	// cleaned up, even if the executions fails.
	remoteRunTmpl = `sh -c 'bin=$(mktemp) && cat - > $bin && chmod +x $bin` +
		` && $bin %s; rm $bin'`
)

// runMeRemote copies the binary of the current process to a remote destination
// and tries to execute it there via SSH. The supplied args are passed to the
// executable when it's run remotely. The binary is copied to a temp file on
// the remote host, and removed again after execution. The dst argument can be
// any valid destination understood by the 'ssh' client executable.
// Does not wait for the command to finish, returns the command.
func (m *Master) runMeRemote(id, dst, args string) error {
	logger.Printf("Starting remote %q on %q with args %q", id, dst, args)
	cmd := exec.Command("ssh", dst, fmt.Sprintf(remoteRunTmpl, args))
	exeFilename, err := os.Executable()
	exe, err := os.Open(exeFilename)
	if err != nil {
		return err
	}
	defer exe.Close()
	remoteLog := log.New(os.Stderr, "["+id+"] ", 0)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	// Background stdout logging
	m.tasks.Add(1)
	go func() {
		defer m.tasks.Done()
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			remoteLog.Print(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			remoteLog.Printf("Error reading output: %v", err)
		}
	}()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	// Background stderr logging
	m.tasks.Add(1)
	go func() {
		defer m.tasks.Done()
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			remoteLog.Print(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			remoteLog.Printf("Error reading error output: %v", err)
		}
	}()
	if err := cmd.Start(); err != nil {
		return err
	}
	if _, err := io.Copy(stdin, exe); err != nil {
		return err
	}
	stdin.Close()
	// Monitor cmd in background and report final result
	m.tasks.Add(1)
	go func() {
		defer m.tasks.Done()
		err := cmd.Wait()
		if err != nil {
			remoteLog.Printf("Failed: %v", err)
		} else {
			remoteLog.Print("Finished")
		}
	}()
	return nil
}

// serve creates a listener, and accepts and serves new network connections.
func (m *Master) serve() error {
	var err error
	m.listener, err = net.Listen("tcp", m.config.Master)
	if err != nil {
		return err
	}
	m.rpcServer = rpc.NewServer()
	m.rpcServer.Register(m.service)
	m.tasks.Add(1)
	go func() {
		defer m.tasks.Done()
		for {
			conn, err := m.listener.Accept()
			if err != nil {
				logger.Printf("Failed to accept RPC connection: %v", err)
				continue
			}
			m.tasks.Add(1)
			go func() {
				defer m.tasks.Done()
				m.rpcServer.ServeConn(conn)
			}()
		}
	}()
	return nil
}

// startReducers starts up all reducers via SSH.
func (m *Master) startReducers() error {
	for i, v := range m.config.Reducers {
		dest := strings.Split(v, ":")[0]
		args := fmt.Sprintf(
			"-mode reduce -listen %s -master %s",
			v,
			m.config.Master,
		)
		id := "reducer-" + strconv.Itoa(i)
		cmd, err := util.RunMeRemote(id, dest, args)
		if err != nil {
			return err
		}
		m.remoteCmds = append(m.remoteCmds, cmd)
	}
	return nil
}

// startMappers starts up all mappers via SSH.
func (m *Master) startMappers() error {
	numMappers := len(m.config.Mappers)
	for i, v := range m.config.Mappers {
		dest := strings.Split(v, ":")[0]
		args := fmt.Sprintf(
			"-mode map -part %d -numPart %d -master %s",
			i,
			numMappers,
			m.config.Master,
		)
		id := "mapper-" + strconv.Itoa(i)
		cmd, err := util.RunMeRemote(id, dest, args)
		if err != nil {
			return err
		}
		m.remoteCmds = append(m.remoteCmds, cmd)
	}
	return nil
}

// startReducing signals all reducers to start reducing the data received from mappers.
func (m *Master) startReducing() error {
	for _, r := range m.reducerMap {
		var err error
		client, err := m.reducerClients.Client(r)
		if err != nil {
			return err
		}
		err = client.Call("Service.Reduce", msg.Empty, msg.Empty)
		if err != nil {
			return err
		}
	}
	return nil
}

// Run steps through the MapReduce job, controlling its execution.
func (m *Master) Run() error {
	numReducers := len(m.config.Reducers)
	numMappers := len(m.config.Mappers)
	logger.Printf(
		"Running MapReduce with %d mappers and %d reducers",
		numMappers,
		numReducers,
	)
	if err := m.serve(); err != nil {
		return err
	}
	logger.Printf("Serving on %q", m.listener.Addr())
	// TODO: start output goroutine
	if err := m.startReducers(); err != nil {
		return err
	}
	logger.Print("All reducers started")
	reducersStarted := 0
	reducersStopped := 0
	for {
		select {
			case 
		}
	}
	// Wait for all reducers to register
	for c := numReducers; c > 0; {
		select {
		case <-m.fail:
			return errors.New("Aborting due to failures")
		case <-m.staging:
			c--
		case <-time.After(time.Minute):
			return errors.New(
				"Timed out waiting for reducers to register",
			)
		}
	}
	logger.Print("All reducers registered")
	// Start mappers, once we have the list of all reducers with their
	// partition
	if err := m.startMappers(); err != nil {
		return err
	}
	logger.Print("All mappers started")
	// Wait for all mappers to finish
	for c := numMappers; c > 0; {
		select {
		case <-m.fail:
			return errors.New("Aborting due to failures")
		case <-m.staging:
			c--
		case <-time.After(time.Hour):
			return errors.New(
				"Timed out waiting for mappers to finish",
			)
		}
	}
	logger.Print("All mappers finished")
	// Tell reducers to reduce all received data from mappers
	if err := m.startReducing(); err != nil {
		return err
	}
	logger.Print("Started reducing")
	// Wait for reducers to finish
	for c := numReducers; c > 0; {
		select {
		case <-m.fail:
			return errors.New("Aborting due to failures")
		case <-m.staging:
			c--
		case <-time.After(time.Hour):
			return errors.New(
				"Timed out waiting for reducers to register",
			)
		}
	}
	logger.Print("All reducers finished")
	// Output final result
	if err := m.outputFn(m.output); err != nil {
		return err
	}
	return nil
}
