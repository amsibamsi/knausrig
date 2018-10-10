package master

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/msg"
)

// Master ...
type Master struct {
	mappers            []string
	numMappers         int
	mappersCount       int64
	mappersFinished    int64
	reducers           []string
	numReducers        int
	reducersCount      int64
	reducerMap         map[int64]string
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
func NewMaster(numMappers, numReducers int, outputFn mapreduce.OutputFn) *Master {
	m := Master{
		numMappers:         numMappers,
		mappersCount:       -1,
		mappersFinished:    0,
		numReducers:        numReducers,
		reducersCount:      -1,
		reducerMap:         make(map[int64]string),
		reducersRegistered: &sync.WaitGroup{},
		reducersFinished:   &sync.WaitGroup{},
		outputDone:         &sync.WaitGroup{},
		output:             make(map[string]string),
		outputLock:         &sync.Mutex{},
		outputFn:           outputFn,
	}
	m.reducersRegistered.Add(numReducers)
	m.reducersFinished.Add(numReducers)
	m.outputDone.Add(1)
	return &m
}

// Ping ...
func (m *Master) Ping(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	return nil
}

// RegisterReducer ...
func (m *Master) RegisterReducer(addrs *string, _ *msg.EmptyMsg) error {
	p := atomic.AddInt64(&m.reducersCount, 1)
	if p >= int64(m.numReducers) {
		return errors.New("No more free reducer range")
	}
	m.reducerMap[p] = *addrs
	m.reducersRegistered.Done()
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

// serve ...
func (m *Master) serve() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go m.rpcServer.ServeConn(conn)
	}
}

// Run ...
func (m *Master) Run() (int, error) {
	log.Printf(
		"Running MapReduce with %d mappers and %d reducers",
		m.numMappers,
		m.numReducers,
	)
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
	go func() {
		m.reducersFinished.Wait()
		if err := m.outputFn(m.output); err != nil {
			log.Printf("Output error: %v", err)
		}
		m.outputDone.Done()
	}()
	port := addr.(*net.TCPAddr).Port
	return port, nil
}

// WaitFinish ...
func (m *Master) WaitFinish() {
	m.reducersFinished.Wait()
	m.outputDone.Wait()
}
