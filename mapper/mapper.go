package mapper

import (
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"

	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/msg"
)

var (
	logger = log.New(os.Stderr, "", 0)
)

// Mapper ...
type Mapper struct {
	mapFn          mapreduce.MapFn
	client         *rpc.Client
	reducers       map[int64]string
	reducerClients map[int64]*rpc.Client
}

// NewMapper ...
func NewMapper(mapFn mapreduce.MapFn) *Mapper {
	return &Mapper{
		mapFn:          mapFn,
		reducerClients: make(map[int64]*rpc.Client),
	}
}

func (m *Mapper) reducerClient(part int64) (*rpc.Client, error) {
	if client, ok := m.reducerClients[part]; ok {
		return client, nil
	}
	addrs := strings.Split(m.reducers[part], ",")
	var conn net.Conn
	var err error
	for _, addr := range addrs {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	client := rpc.NewClient(conn)
	m.reducerClients[part] = client
	return client, nil
}

// Run ...
func (m *Mapper) Run(masterAddrs string) error {
	var err error
	addrs := strings.Split(masterAddrs, ",")
	var conn net.Conn
	for _, addr := range addrs {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	m.client = rpc.NewClient(conn)
	defer m.client.Close()
	info := new(msg.MapperInfo)
	if err := m.client.Call("Master.GetMapperInfo", msg.Empty, info); err != nil {
		return err
	}
	m.reducers = info.Reducers
	out := make(chan [2]string)
	go func() {
		m.mapFn(info.Partition, out)
		close(out)
	}()
	for e := range out {
		h := fnv.New64a()
		h.Write([]byte(e[0]))
		hi := h.Sum64()
		p := hi % uint64(len(info.Reducers))
		c, err := m.reducerClient(int64(p))
		if err != nil {
			return err
		}
		c.Call("Reducer.Element", &e, msg.Empty)
	}
	if err := m.client.Call("Master.MapperFinished", msg.Empty, msg.Empty); err != nil {
		return err
	}
	return nil
}
