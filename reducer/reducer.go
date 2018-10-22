// Package reducer contains the reducer task of a MapReduce job.
package reducer

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"sync/atomic"

	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/msg"
)

const (
	chanSize = 10
)

var (
	logger = log.New(os.Stderr, "", log.Lmicroseconds)
)

// Service holds the methods to expose to mappers and the master nvia net/rpc
// package.
type Service struct {
	r *Reducer
}

// Element receives a data element from mappers.
func (s *Service) Element(e [2]string, _ *msg.EmptyMsg) error {
	s.r.input <- e
	atomic.AddUint64(&s.r.inputCount, 1)
	return nil
}

// Finish signals the reducer that all data from mappers has been sent and the
// reducer should finish up.
func (s *Service) Finish(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	close(s.r.input)
	logger.Print("Closed input")
	return nil
}

// Reducer is the service to receive keyed data from mappers and reduce it per
// key.
type Reducer struct {
	listen       string
	service      *Service
	listener     net.Listener
	rpcServer    *rpc.Server
	master       string
	masterClient *rpc.Client
	reduceFn     mapreduce.ReduceFn
	input        chan [2]string
	inputCount   uint64
}

// NewReducer returns a new reducer.
func NewReducer(listen, master string, reduceFn mapreduce.ReduceFn) *Reducer {
	r := Reducer{
		listen:   listen,
		master:   master,
		reduceFn: reduceFn,
		input:    make(chan [2]string, chanSize),
	}
	r.service = &Service{&r}
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
	go func() {
		for {
			conn, err := r.listener.Accept()
			if err != nil {
				logger.Printf("Failed to accept RPC connection: %v", err)
				continue
			}
			go func() {
				r.rpcServer.ServeConn(conn)
			}()
		}
	}()
	logger.Printf("Serving on %q", r.listener.Addr())
	return nil
}

// connectMaster creates RPC client and connects it to the master.
func (r *Reducer) connectMaster() error {
	conn, err := net.Dial("tcp", r.master)
	if err != nil {
		return err
	}
	r.masterClient = rpc.NewClient(conn)
	logger.Print("Connected to master")
	return nil
}

// reduce starts the custom reduce function
func (r *Reducer) reduce() error {
	logger.Print("Reducing ...")
	output := make(chan [2]string, chanSize)
	go func() {
		if err := r.reduceFn(r.input, output); err != nil {
			logger.Print(err)
		}
		close(output)
	}()
	outCount := 0
	for e := range output {
		outCount++
		if err := r.masterClient.Call("Service.Output", e, msg.Empty); err != nil {
			logger.Print(err)
		}
	}
	logger.Printf("Received %d elements, sent %d elements", r.inputCount, outCount)
	return nil
}

// Run starts up the listener, waits for data from the mappers, reduces the
// data and sends the result to the master.
func (r *Reducer) Run() error {
	if err := r.serve(); err != nil {
		return err
	}
	if err := r.connectMaster(); err != nil {
		return err
	}
	if err := r.masterClient.Call("Service.ReducerStart", msg.Empty, msg.Empty); err != nil {
		return err
	}
	logger.Print("Registered at master")
	if err := r.reduce(); err != nil {
		return err
	}
	if err := r.masterClient.Call("Service.ReducerStop", msg.Empty, msg.Empty); err != nil {
		return err
	}
	logger.Print("Finished")
	return nil
}

// Main runs the reducer and logs any error.
func (r *Reducer) Main() {
	if err := r.Run(); err != nil {
		logger.Fatal(err)
	}
}
