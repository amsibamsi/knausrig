package master

import (
	"fmt"
	"log"
	"net"
	"net/rpc"

	"github.com/amsibamsi/knausrig/util"
)

// Master ...
type Master struct {
	mappers   []string
	reducers  []string
	listener  net.Listener
	rpcServer *rpc.Server
}

// NewMaster ...
func NewMaster(mapperAddrs, reducerAddrs []string) *Master {
	return &Master{}
}

// Ping ...
func (m *Master) Ping() error {
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
func (m *Master) Run() error {
	var err error
	m.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		return err
	}
	addr := m.listener.Addr()
	log.Printf("Master listening on %q", addr)
	m.rpcServer = rpc.NewServer()
	m.rpcServer.Register(m)
	go m.serve()
	port := addr.(*net.TCPAddr).Port
	addrs := ""
	ips, err := util.LocalIPs()
	if err != nil {
		return err
	}
	for _, ip := range ips {
		if addrs == "" {
			addrs = ip.String()
		} else {
			addrs = addrs + "," + ip.String()
		}
	}
	for _, reducer := range m.reducers {
		args := fmt.Sprintf(
			"-operation reduce -masterPort %d -masterAddrs %s",
			port,
			addrs,
		)
		if err := util.RunMeRemote(reducer, args); err != nil {
			return err
		}
	}
	for _, mapper := range m.reducers {
		args := fmt.Sprintf(
			"-operation map -masterPort %d -masterAddrs %s",
			port,
			addrs,
		)
		if err := util.RunMeRemote(mapper, args); err != nil {
			return err
		}
	}
	return nil
}
