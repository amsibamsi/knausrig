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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

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
	logger = log.New(os.Stderr, "master", 0)
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
	s.m.reducersRegistered.Done()
	log.Printf("New reducer %q for partition %q", *addr, p)
	return nil
}

// Master initializes everything and controls the execution of a MapReduce job.
type Master struct {
	config             *cfg.Config
	lock               *sync.Mutex
	mappers            []string
	numMappers         int
	mappersCount       int64
	mappersFinished    int64
	reducers           []string
	numReducers        int
	reducersCount      int64
	reducerMap         map[int]string
	reducersRegistered *sync.WaitGroup
	reducersFinished   *sync.WaitGroup
	outputDone         *sync.WaitGroup
	listener           net.Listener
	rpcServer          *rpc.Server
	output             map[string]string
	outputLock         *sync.Mutex
	outputFn           mapreduce.OutputFn
}

// NewMaster ...
func NewMaster(outputFn mapreduce.OutputFn) (*Master, error) {
	cfg, err := cfg.FromFile(*config)
	if err != nil {
		return nil, err
	}
	m := Master{
		config:             cfg,
		lock:               &sync.Mutex{},
		numMappers:         len(cfg.Mappers),
		mappersCount:       -1,
		mappersFinished:    0,
		numReducers:        len(cfg.Reducers),
		reducersCount:      -1,
		reducerMap:         make(map[int]string),
		reducersRegistered: &sync.WaitGroup{},
		reducersFinished:   &sync.WaitGroup{},
		outputDone:         &sync.WaitGroup{},
		output:             make(map[string]string),
		outputLock:         &sync.Mutex{},
		outputFn:           outputFn,
	}
	m.reducersRegistered.Add(len(cfg.Reducers))
	m.reducersFinished.Add(len(cfg.Reducers))
	m.outputDone.Add(1)
	return &m, nil
}

// serveOn creates a listener, and accepts and serves new network connections.
func (m *Master) serveOn(addr string) error {
	var err error
	m.listener, err = net.Listen("tcp", m.config.Master)
	if err != nil {
		return err
	}
	m.rpcServer = rpc.NewServer()
	m.rpcServer.Register(m)
	go func() {
		for {
			conn, err := m.listener.Accept()
			if err != nil {
				log.Printf("Failed to accept RPC connection: %v", err)
				continue
			}
			go m.rpcServer.ServeConn(conn)
		}
	}()
	log.Printf("Serving on %q", m.listener.Addr())
	return nil
}

// ReducerFinished ...
func (m *Master) ReducerFinished(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	m.reducersFinished.Done()
	return nil
}

// GetMapperInfo ...
func (m *Master) GetMapperInfo(_ *msg.EmptyMsg, mapInfo *msg.MapperInfo) error {
	m.reducersRegistered.Wait()
	mapInfo.Partition = atomic.AddInt64(&m.mappersCount, 1)
	if mapInfo.Partition >= int64(m.numMappers) {
		return errors.New("No more free mapper range")
	}
	mapInfo.Reducers = m.reducerMap
	log.Print("Sending info to mapper")
	return nil
}

// MapperFinished ...
func (m *Master) MapperFinished(req *msg.EmptyMsg, resp *msg.EmptyMsg) error {
	c := atomic.AddInt64(&m.mappersFinished, 1)
	if c > int64(m.numMappers) {
		return errors.New("All mappers already finished")
	}
	// TODO: separate this with wait group
	if c == int64(m.numMappers) {
		for _, r := range m.reducerMap {
			addrs := strings.Split(r, ",")
			var conn net.Conn
			var err error
			for _, addr := range addrs {
				conn, err = net.Dial("tcp", addr)
				if err == nil {
					break
				}
			}
			if err != nil {
				return err
			}
			client := rpc.NewClient(conn)
			if err := client.Call("Reducer.Start", msg.Empty, msg.Empty); err != nil {
				return err
			}
			client.Close()
		}
	}
	return nil
}

// Output ...
func (m *Master) Output(req map[string]string, resp *msg.EmptyMsg) error {
	m.outputLock.Lock()
	defer m.outputLock.Unlock()
	for k, v := range req {
		m.output[k] = v
	}
	return nil
}

// Run ...
func (m *Master) Run() error {
	log.Printf(
		"Running MapReduce with %d mappers and %d reducers",
		len(m.config.Mappers),
		len(m.config.Reducers),
	)
	var err error
	go func() {
		m.reducersFinished.Wait()
		if err := m.outputFn(m.output); err != nil {
			log.Printf("Output error: %v", err)
		}
		m.outputDone.Done()
	}()
	for i, reducer := range m.config.Reducers {
		args := fmt.Sprintf(
			"-mode reduce -master %s",
			m.config.Master,
		)
		id := "reducer-" + strconv.Itoa(i)
		if _, err := util.RunMeRemote(id, reducer, args); err != nil {
			return err
		}
	}
	for i, mapper := range m.config.Mappers {
		args := fmt.Sprintf(
			"-mode map -master %s",
			m.config.Master,
		)
		id := "mapper-" + strconv.Itoa(i)
		if _, err := util.RunMeRemote(id, mapper, args); err != nil {
			return err
		}
	}
	m.reducersFinished.Wait()
	m.outputDone.Wait()
	return nil
}
