// Package reducer contains the reducer task of a MapReduce job.
package reducer

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/msg"
)

var (
	logger = log.New(os.Stderr, "", 0)
)

// Service holds the RPC service exposed to other MapReduce processes. All
// exported methods on this struct are considered for exposing via RPC.
type Service struct {
	r *Reducer
}

// NewService returns a new service for the given reducer.
func NewService(r *Reducer) *Service {
	return &Service{r}
}

// Element receives a data element from mappers.
func (s *Service) Element(e [2]string, _ *msg.EmptyMsg) error {
	s.r.lock.Lock()
	defer s.r.lock.Unlock()
	if _, ok := s.r.elements[e[0]]; !ok {
		s.r.elements[e[0]] = make([]string, 0)
	}
	s.r.elements[e[0]] = append(s.r.elements[e[0]], e[1])
	return nil
}

// Reduce signals starting to reduce, after all data from mappers has been received.
func (s *Service) Reduce(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	s.r.staging <- struct{}{}
	return nil
}

// Reducer is the service to receive keyed data from mappers and reduce it per
// key.
type Reducer struct {
	service   *Service
	master    string
	listen    string
	reduceFn  mapreduce.ReduceFn
	listener  net.Listener
	rpcServer *rpc.Server
	server    *rpc.Server
	client    *rpc.Client
	elements  map[string][]string
	lock      *sync.Mutex
	staging   chan struct{}
}

// NewReducer returns a new reducer.
func NewReducer(listen, master string, reduceFn mapreduce.ReduceFn) *Reducer {
	r := Reducer{
		master:   master,
		listen:   listen,
		reduceFn: reduceFn,
		elements: make(map[string][]string),
		lock:     &sync.Mutex{},
		staging:  make(chan struct{}),
	}
	r.service = NewService(&r)
	return &r
}

// serve creates a listener, and accepts and serves new network connections.
func (r *Reducer) serve() error {
	var err error
	r.listener, err = net.Listen("tcp", r.listen)
	if err != nil {
		return err
	}
	r.rpcServer = rpc.NewServer()
	r.rpcServer.Register(r.service)
	go func(r *Reducer) {
		for {
			conn, err := r.listener.Accept()
			if err != nil {
				logger.Printf("Failed to accept RPC connection: %v", err)
				continue
			}
			go r.rpcServer.ServeConn(conn)
		}
	}(r)
	return nil
}

// connectMaster creates RPC client and connects it to the master.
func (r *Reducer) connectMaster() error {
	conn, err := net.Dial("tcp", r.master)
	if err != nil {
		return err
	}
	r.client = rpc.NewClient(conn)
	return nil
}

// reduce reduces all the data received per key, and sends it to the master.
func (r *Reducer) reduce() error {
	out := make(map[string]string)
	for k, v := range r.elements {
		logger.Printf("Reducing key %q with %d elements", k, len(v))
		e, err := r.reduceFn(k, v)
		if err != nil {
			return err
		}
		out[k] = e
	}
	if err := r.client.Call("Service.Output", &out, msg.Empty); err != nil {
		return err
	}
	return nil
}

// Run starts up the listener, waits for data from the mappers, reduces the
// data and sends the result to the master.
func (r *Reducer) Run() error {
	if err := r.serve(); err != nil {
		return err
	}
	logger.Printf("Serving on %q", r.listener.Addr())
	if err := r.connectMaster(); err != nil {
		return err
	}
	logger.Print("Connected to master")
	addr := r.listener.Addr().String()
	// Register at master
	if err := r.client.Call("Service.NewReducer", &addr, msg.Empty); err != nil {
		return err
	}
	logger.Print("Registered at master")
	// Wait ok from master to reduce (after all mappers have finished)
	select {
	case <-r.staging:
	case <-time.After(3600 * time.Second):
		return errors.New("Waiting for mappers timed out")
	}
	// Reduce the data
	if err := r.reduce(); err != nil {
		return err
	}
	if err := r.client.Call("Service.ReducerFinished", msg.Empty, msg.Empty); err != nil {
		return err
	}
	logger.Print("Finished")
	return nil
}
