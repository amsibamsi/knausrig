package reducer

import (
	"log"
	"net"
	"net/rpc"
	"strings"
	"sync"

	"github.com/amsibamsi/knausrig"
	"github.com/amsibamsi/knausrig/msg"
)

// Reducer ...
type Reducer struct {
	reduceFn knausrig.ReduceFn
	server   *rpc.Server
	client   *rpc.Client
	done     *sync.WaitGroup
	elements map[string]string
	lock     *sync.Mutex
}

// NewReducer ...
func NewReducer(reduceFn kanusrig.ReduceFn) *Reducer {
	r := &Reducer{
		reduceFn: reduceFn,
		done:     *sync.WaitGroup{},
		elements: make(map[string]string),
		lock:     &sync.Mutex{},
	}
	r.done.Add(1)
	return &r
}

func (r *Reducer) serve() {
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		r.server.ServeConn(conn)
	}
}

// Element ...
func (r *Reducer) Element(e [2]string, _ msg.Empty) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.elements[e[0]]; !ok {
		r.elements[e[0]] = make([]string)
	}
	r.elements[e[0]] = append(r.elements[e[0]], e[1])
	return nil
}

// Start ...
func (r *Reducer) Start(_ msg.Emtpy, _ msg.Empty) error {
	out := make(map[string]string)
	for k, v := range r.elements {
		e, err := r.reduceFn(e)
		if err != nil {
			return err
		}
		out[k] = e
	}
	if err := r.client.Call("Master.Output", &out, msg.Empty); err != nil {
		return err
	}
	r.done.Done()
	return nil
}

// Run ...
func (r *Reducer) Run(masterAddrs string) error {
	r.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	r.server = rpc.NewServer()
	r.server.Register(r)
	go r.serve()
	addrs := strings.Split(masterAddrs, ",")
	var err error
	var conn net.Addr
	for _, addr := range addrs {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	r.client = rpc.NewClient(conn)
	defer r.client.Close()
	empty := new(msg.EmptyMsg)
	info := new(msg.PartitionInfo)
	if err := m.client.Call("Master.RegisterReducer", empty, info); err != nil {
		return err
	}
	r.done.Wait()
	if err := r.client.Call("Master.ReducerFinished", msg.Empty, msg.Empty); err != nil {
		return err
	}
}
