package mapper

import (
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"strings"

	"github.com/amsibamsi/knausrig"
	"github.com/amsibamsi/knausrig/msg"
)

// Mapper ...
type Mapper struct {
	mapFn          knausrig.MapFn
	listener       net.Listener
	server         *rpc.Server
	client         *rpc.Client
	reducers       map[int64]string
	reducerClients map[int64]*rpc.Client
}

// NewMapper ...
func NewMapper(mapFn knausrig.MapFn) *Mapper {
	return &Mapper{
		mapFn:          mapFn,
		reducerClients: make(map[int64]*rpc.Client),
	}
}

func (m *Mapper) serve() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		m.server.ServeConn(conn)
	}
}

func (m *Mapper) reducerClient(part int64) (*rpc.Client, error) {
	if client, ok := m.reducerClients[part]; ok {
		return client, nil
	}
	conn, err := net.Dial("tcp", m.reducers[part])
	if err != nil {
		return nil, err
	}
	client := rpc.NewClient(conn)
	m.reducers[part] = client
	return client, nil
}

// Run ...
func (m *Mapper) Run(masterAddrs string) error {
	m.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	m.server = rpc.NewServer()
	m.server.Register(m)
	go m.serve()
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
	m.client = rpc.NewClient(conn)
	defer m.client.Close()
	empty := new(msg.EmptyMsg)
	info := new(msg.MapperInfo)
	if err := m.client.Call("Master.GetMapperInfo", empty, info); err != nil {
		return err
	}
	m.reducers = info.Reducers
	out := make(chan [2]string)
	go func() {
		m.mapFn(info.PartInfo.Index, out)
		close(out)
	}()
	for e := range out {
		h := fnv.New64a()
		h.Write([]byte(e[0]))
		hi := h.Sum64()
		p := hi % len(info.Reducers)
		c, err := m.reducerClient(p)
		if err != nil {
			return err
		}
		c.Call("Reducer.Element", &e, msg.Empty)
	}
	if err := m.client.Call("Master.MapperFinished", msg.Empty, msg.Empty); err != nil {
		return err
	}
}
