package reducer

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/msg"
	"github.com/amsibamsi/knausrig/util"
)

var (
	logger = log.New(os.Stderr, "", 0)
)

// Reducer ...
type Reducer struct {
	reduceFn mapreduce.ReduceFn
	listener net.Listener
	server   *rpc.Server
	client   *rpc.Client
	done     *sync.WaitGroup
	elements map[string][]string
	lock     *sync.Mutex
}

// NewReducer ...
func NewReducer(reduceFn mapreduce.ReduceFn) *Reducer {
	r := Reducer{
		reduceFn: reduceFn,
		done:     &sync.WaitGroup{},
		elements: make(map[string][]string),
		lock:     &sync.Mutex{},
	}
	r.done.Add(1)
	return &r
}

func (r *Reducer) serve() {
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			logger.Printf("Error accepting connection: %v", err)
			continue
		}
		go r.server.ServeConn(conn)
	}
}

// Element ...
func (r *Reducer) Element(e [2]string, _ *msg.EmptyMsg) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.elements[e[0]]; !ok {
		r.elements[e[0]] = make([]string, 0)
	}
	r.elements[e[0]] = append(r.elements[e[0]], e[1])
	logger.Print("Got new element")
	return nil
}

// Start ...
func (r *Reducer) Start(_ *msg.EmptyMsg, _ *msg.EmptyMsg) error {
	logger.Print("Starting to reduce ...")
	out := make(map[string]string)
	for k, v := range r.elements {
		logger.Printf("Reducing key %q with %d elements", k, len(v))
		e, err := r.reduceFn(k, v)
		if err != nil {
			return err
		}
		out[k] = e
	}
	if err := r.client.Call("Master.Output", &out, msg.Empty); err != nil {
		return err
	}
	r.done.Done()
	logger.Print("Reducing finished")
	return nil
}

// Run ...
func (r *Reducer) Run(masterAddrs string) error {
	var err error
	r.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		return err
	}
	r.server = rpc.NewServer()
	r.server.Register(r)
	port := r.listener.Addr().(*net.TCPAddr).Port
	go r.serve()
	logger.Printf("Serving on %q", r.listener.Addr())
	var conn net.Conn
	for _, addr := range strings.Split(masterAddrs, ",") {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	r.client = rpc.NewClient(conn)
	defer r.client.Close()
	logger.Printf("Connected to master on %q", conn.RemoteAddr())
	addrs := ""
	ips, err := util.LocalIPs()
	if err != nil {
		return err
	}
	for _, ip := range ips {
		if addrs != "" {
			addrs = addrs + ","
		}
		addrs = addrs + ip.String() + ":" + strconv.Itoa(port)
	}
	if err := r.client.Call("Master.RegisterReducer", &addrs, msg.Empty); err != nil {
		return err
	}
	logger.Print("Registered at master")
	r.done.Wait()
	if err := r.client.Call("Master.ReducerFinished", msg.Empty, msg.Empty); err != nil {
		return err
	}
	logger.Print("Finished")
	return nil
}
