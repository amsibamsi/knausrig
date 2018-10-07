package master

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"

	"github.com/amsibamsi/knausrig/msg"
)

// Master ...
type Master struct {
	mappers          []string
	numMappers       int
	mappersCount     int64
	mappersFinished  int64
	reducers         []string
	numReducers      int
	reducersCount    int64
	reducerMap       map[int64]string
	reducersFinished *sync.WaitGroup
	outputDone       *sync.WaitGroup
	listener         net.Listener
	rpcServer        *rpc.Server
}

// NewMaster ...
func NewMaster(numMappers, numReducers int) *Master {
	m := Master{
		numMappers:       numMappers,
		mappersCount:     -1,
		mappersFinished:  0,
		numReducers:      numReducers,
		reducersCount:    -1,
		reducerMap:       make(map[int64]string),
		reducersFinished: &sync.WaitGroup{},
		outputDone:       &sync.WaitGroup{},
	}
	m.reducersFinished.Add(numReducers)
	m.outputDone.Add(1)
	return &m
}

// RegisterReducer ...
func (m *Master) RegisterReducer(addrs *string, _ *msg.EmptyMsg) error {
	p := atomic.AddInt64(&m.reducersCount, 1)
	if p > int64(m.numReducers) {
		return errors.New("No more free reducer range")
	}
	m.reducerMap[p] = *addrs
	log.Printf("New reducer %q for partition %v", *addrs, p)
	return nil
}

// ReducerFinished ...
func (m *Master) ReducerFinished(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	m.reducersFinished.Done()
	return nil
}

// GetMapperInfo ...
func (m *Master) GetMapperInfo(_ *msg.EmptyMsg, mapInfo *msg.MapperInfo) error {
	mapInfo.Partition = atomic.AddInt64(&m.mappersCount, 1)
	if m.mappersCount > int64(m.numMappers) {
		return errors.New("No more free mapper range")
	}
	mapInfo.Reducers = m.reducerMap
	return nil
}

// MapperFinished ...
func (m *Master) MapperFinished(req *msg.EmptyMsg, resp *msg.EmptyMsg) error {
	c := atomic.AddInt64(&m.mappersFinished, 1)
	if c > int64(m.numMappers) {
		return errors.New("All mappers already finished")
	}
	if c == int64(m.numMappers) {
		for _, r := range m.reducerMap {
			conn, err := net.Dial("tcp", r)
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
	fmt.Printf("Final output: %+v\n", req)
	return nil
}

// serve ...
func (m *Master) serve() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		m.rpcServer.ServeConn(conn)
	}
}

// Run ...
func (m *Master) Run() (int, error) {
	var err error
	m.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	addr := m.listener.Addr()
	m.rpcServer = rpc.NewServer()
	m.rpcServer.Register(m)
	go m.serve()
	log.Printf("Serving on %q", addr)
	port := addr.(*net.TCPAddr).Port
	return port, nil
}

// WaitFinish ...
func (m *Master) WaitFinish() {
	m.reducersFinished.Wait()
	m.outputDone.Wait()
}
