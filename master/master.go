// Package master contains the master process initiating and coordinating the
// whole process of executing a MapReduce job.
package master

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/amsibamsi/knausrig/cfg"
	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/msg"
	"github.com/amsibamsi/knausrig/util"
)

var (
	config = flag.String(
		"config",
		"./config.json",
		"Configuration file for master (modes: master)",
	)
	logger = log.New(os.Stderr, "[master] ", 0)
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
	logger.Printf("New reducer %q for partition %q", *addr, p)
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
	logger.Printf("New mapper for partition %q", p)
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
}

// NewMaster ...
func NewMaster(outputFn mapreduce.OutputFn) (*Master, error) {
	cfg, err := cfg.FromFile(*config)
	if err != nil {
		return nil, err
	}
	m := Master{
		config:         cfg,
		lock:           &sync.Mutex{},
		reducerMap:     make(map[int]string),
		output:         make(map[string]string),
		outputFn:       outputFn,
		remoteCmds:     make([]*exec.Cmd, 1),
		fail:           make(chan struct{}),
		staging:        make(chan struct{}),
		reducerClients: util.NewRPCClients(),
	}
	m.service = NewService(&m)
	return &m, nil
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
	go func(m *Master) {
		for {
			conn, err := m.listener.Accept()
			if err != nil {
				logger.Printf("Failed to accept RPC connection: %v", err)
				continue
			}
			go m.rpcServer.ServeConn(conn)
		}
	}(m)
	return nil
}

// watchCmd waits on a command and reports if the command fails.
func (m *Master) watchCmd(id string, cmd *exec.Cmd) {
	err := cmd.Wait()
	if err != nil {
		logger.Printf("Remote command on %s failed: %v", id, err)

		m.fail <- struct{}{}
	}
}

// startReducers runs the remote command on every reducer machine.
func (m *Master) startReducers() error {
	for i, reducer := range m.config.Reducers {
		args := fmt.Sprintf(
			"-mode reduce -master %s",
			m.config.Master,
		)
		id := "reducer-" + strconv.Itoa(i)
		cmd, err := util.RunMeRemote(id, reducer, args)
		if err != nil {
			return err
		}
		m.remoteCmds = append(m.remoteCmds, cmd)
		go m.watchCmd(id, cmd)
	}
	return nil
}

// startMappers runs the remote command on every mapper machine.
func (m *Master) startMappers() error {
	for i, mapper := range m.config.Mappers {
		args := fmt.Sprintf(
			"-mode map -master %s",
			m.config.Master,
		)
		id := "mapper-" + strconv.Itoa(i)
		cmd, err := util.RunMeRemote(id, mapper, args)
		if err != nil {
			return err
		}
		m.remoteCmds = append(m.remoteCmds, cmd)
		go m.watchCmd(id, cmd)
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

// Run starts up the listener, starts reducers/mappers, runs the different
// stages of the MapReduce process, and outputs the final result.
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
	if err := m.startReducers(); err != nil {
		return err
	}
	logger.Print("All reducers started")
	// Wait for all reducers to register
	for c := numReducers; c > 0; {
		select {
		case <-m.fail:
			return errors.New("Aborting due to failures")
		case <-m.staging:
			c--
		case <-time.After(60 * time.Second):
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
		case <-time.After(3600 * time.Second):
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
		case <-time.After(3600 * time.Second):
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
