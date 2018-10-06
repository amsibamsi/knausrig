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
		mappersCount:     0,
		mappersFinished:  0,
		numReducers:      numReducers,
		reducersCount:    0,
		reducerMap:       make(map[int64]string),
		reducersFinished: &sync.WaitGroup{},
		outputDone:       &sync.WaitGroup{},
	}
	m.reducersFinished.Add(numReducers)
	m.outputDone.Add(1)
	return &m
}

// RegisterReducer ...
func (m *Master) RegisterReducer(reducer string, part *msg.PartitionInfo) error {
	part.Index = atomic.AddInt64(&m.reducersCount, 1)
	if m.reducersCount > int64(m.numReducers) {
		return errors.New("No more free reducer range")
	}
	part.Max = int64(m.numReducers)
	m.reducerMap[part.Index] = reducer
	return nil
}

// GetMapperInfo ...
func (m *Master) GetMapperInfo(req *msg.EmptyMsg, mapInfo *msg.MapperInfo) error {
	mapInfo.PartInfo.Index = atomic.AddInt64(&m.mappersCount, 1)
	if m.mappersCount > int64(m.numMappers) {
		return errors.New("No more free mapper range")
	}
	mapInfo.PartInfo.Max = int64(m.numMappers)
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

// Log ...
func (m *Master) Log(msg *string, resp *msg.EmptyMsg) error {
	log.Printf("Remote: %s", *msg)
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
	log.Printf("Master listening on %q", addr)
	m.rpcServer = rpc.NewServer()
	m.rpcServer.Register(m)
	go m.serve()
	port := addr.(*net.TCPAddr).Port
	return port, nil
}

// WaitFinish ...
func (m *Master) WaitFinish() {
	m.reducersFinished.Wait()
	m.outputDone.Wait()
}
