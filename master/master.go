// Package master contains the master process initiating and coordinating the
// whole process of executing a MapReduce job.
package master

import (
	"bufio"
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

	"github.com/amsibamsi/knausrig/cfg"
	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/msg"
	"github.com/amsibamsi/knausrig/util"
)

const (
	chanSize = 10
)

var (
	logger = log.New(os.Stderr, "[master] ", log.Lmicroseconds)
)

const (
	eventMapperStop event = iota
	eventReducerStart
	eventReducerStop
)

// event is processed by the master, multiple in sequence, to control the flow
// of a MapReduce job.
type event int

// Service holds the methods to expose to mappers/reducers via net/rpc package.
type Service struct {
	m *Master
}

// ReducerStart notifies the master of a new reducer being online.
func (s *Service) ReducerStart(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	logger.Print("Reducer started")
	s.m.events <- eventReducerStart
	return nil
}

// ReducerStop notifies the master that a reducer has finished.
func (s *Service) ReducerStop(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	logger.Print("Reducer stopped")
	s.m.events <- eventReducerStop
	return nil
}

// MapperStart sends the list of reducers to the mapper.
func (s *Service) MapperStart(_ *msg.EmptyMsg, reducerMap *map[int]string) error {
	logger.Print("Mapper started")
	*reducerMap = s.m.reducerMap
	return nil
}

// MapperStop indicates that a mapper has finished processing and sent all data
// to reducers.
func (s *Service) MapperStop(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	logger.Print("Mapper stopped")
	s.m.events <- eventMapperStop
	return nil
}

// Output receives output from a reducer. Assuming that there is going to be at
// most one call per key.
func (s *Service) Output(req [2]string, _ *msg.EmptyMsg) error {
	s.m.output <- req
	return nil
}

// Master initializes everything and controls the execution of a MapReduce job.
type Master struct {
	config         *cfg.Config
	tasks          *sync.WaitGroup
	events         chan event
	listener       net.Listener
	rpcServer      *rpc.Server
	service        *Service
	reducerMap     map[int]string
	reducerClients *util.RPCClients
	outputFn       mapreduce.OutputFn
	output         chan [2]string
}

// NewMaster returns a new master.
func NewMaster(config *cfg.Config, outputFn mapreduce.OutputFn) *Master {
	m := Master{
		config:         config,
		tasks:          &sync.WaitGroup{},
		events:         make(chan event, chanSize),
		reducerMap:     make(map[int]string),
		reducerClients: util.NewRPCClients(),
		outputFn:       outputFn,
		output:         make(chan [2]string, chanSize),
	}
	m.service = &Service{&m}
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

// runMeRemote uses SSH to copy the binary of the current process to a remote
// destination and tries to execute it there. The supplied args are passed to
// the executable when it's run remotely. The binary is copied to a temp file
// on the remote host, and removed again after execution (except when the
// remote process is killed). The dst argument can be any valid destination
// understood by the 'ssh' client executable.  Does not wait for the command to
// finish and starts some goroutines to pipe stdout/stderr output to the log,
// and to log final results of the process.
func (m *Master) runMeRemote(id, dst, args string) error {
	logger.Printf("%s: starting on %q with args %q", id, dst, args)
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
			logger.Printf("%s: error reading output: %v", id, err)
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
			logger.Printf("%s: error reading error output: %v", id, err)
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
			logger.Printf("%s: failed: %v", id, err)
		} else {
			logger.Printf("%s: success", id)
		}
	}()
	return nil
}

// result takes data from the output channel, that reducers send to, and
// outputs/writes the final result. This starts a goroutine.
func (m *Master) result() {
	m.tasks.Add(1)
	go func() {
		defer m.tasks.Done()
		if err := m.outputFn(m.output); err != nil {
			logger.Printf("Output failed: %v", err)
		}
	}()
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
	go func() {
		for {
			conn, err := m.listener.Accept()
			if err != nil {
				logger.Printf("Failed to accept RPC connection: %v", err)
				continue
			}
			go func() {
				m.rpcServer.ServeConn(conn)
			}()
		}
	}()
	logger.Printf("Serving on %q", m.listener.Addr())
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
		err := m.runMeRemote(id, dest, args)
		if err != nil {
			return err
		}
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
		err := m.runMeRemote(id, dest, args)
		if err != nil {
			return err
		}
	}
	return nil
}

// finishReducers signals all reducers that mapping has finished and they can
// finish as well.
func (m *Master) finishReducers() error {
	for _, r := range m.reducerMap {
		client, err := m.reducerClients.Client(r)
		if err != nil {
			return err
		}
		err = client.Call("Service.Finish", msg.Empty, msg.Empty)
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
	// Start pipeline in reverse order. Output needs to ready before reducers
	// send, reducers need to be ready before mappers send.
	m.result()
	if err := m.startReducers(); err != nil {
		return err
	}
	// Use events from here on
	countReducers := 0
	countMappers := 0
EventLoop:
	for {
		switch event := <-m.events; event {
		case eventReducerStart:
			countReducers++
			if countReducers >= numReducers {
				logger.Print("All reducers ready, starting mappers")
				if err := m.startMappers(); err != nil {
					return err
				}
			}
		case eventMapperStop:
			countMappers++
			if countMappers >= numMappers {
				logger.Print("All mappers finished, signaling reducers")
				if err := m.finishReducers(); err != nil {
					return err
				}
			}
		case eventReducerStop:
			countReducers--
			if countReducers <= 0 {
				logger.Print("All reducers finished, closing output")
				close(m.output)
				break EventLoop
			}
		}
	}
	logger.Print("Waiting for tasks to finish")
	m.tasks.Wait()
	logger.Print("Finished")
	return nil
}

// Main runs the master and logs any error.
func (m *Master) Main() {
	if err := m.Run(); err != nil {
		logger.Fatal(err)
	}
}
