package master

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"

	"github.com/amsibamsi/knausrig/msg"
)

// Master ...
type Master struct {
	mappers       []string
	numMappers    int
	mappersCount  int64
	reducers      []string
	numReducers   int
	reducersCount int64
	reducerMap    map[int64]net.TCPAddr
	finished      *sync.WaitGroup
	listener      net.Listener
	rpcServer     *rpc.Server
}

// NewMaster ...
func NewMaster(numMappers, numReducers int) *Master {
	return &Master{
		numMappers:    numMappers,
		mappersCount:  int64(numMappers),
		numReducers:   numReducers,
		reducersCount: int64(numReducers),
		reducerMap:    make(map[int64]net.TCPAddr),
		finished:      &sync.WaitGroup{},
	}
}

// RegisterReducer ...
func (m *Master) RegisterReducer(reducer *net.TCPAddr, part *msg.PartitionInfo) error {
	if m.reducersCount <= 0 {
		return errors.New("No more free reducer range")
	}
	part.Index = atomic.AddInt64(&m.reducersCount, -1)
	part.Max = int64(m.numReducers)
	m.reducerMap[part.Index] = *reducer
	m.finished.Add(1)
	return nil
}

// GetMapperInfo ...
func (m *Master) GetMapperInfo(req *msg.EmptyMsg, mapInfo *msg.MapperInfo) error {
	if m.mappersCount <= 0 {
		return errors.New("No more free mapper range")
	}
	mapInfo.PartitionInfo.Index = atomic.AddInt64(&m.mappersCount, -1)
	mapInfo.PartitionInfo.Max = int64(m.numMappers)
	mapInfo.Reducers = m.reducerMap
	m.finished.Add(1)
	return nil
}

// Finished ...
func (m *Master) Finished(req *msg.EmptyMsg, resp *msg.EmptyMsg) error {
	m.finished.Done()
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
	m.finished.Wait()
}
