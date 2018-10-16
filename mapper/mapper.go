// Package mapper contains the mapper task of a MapReduce job.
package mapper

import (
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"

	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/msg"
	"github.com/amsibamsi/knausrig/util"
)

var (
	logger = log.New(os.Stderr, "", 0)
)

// Mapper is the service to input and map data in the first place.
type Mapper struct {
	master         string
	mapFn          mapreduce.MapFn
	masterClient   *rpc.Client
	reducers       map[int]string
	partition      int
	reducerClients *util.RPCClients
	output         chan [2]string
}

// NewMapper returns a new mapper.
func NewMapper(master string, mapFn mapreduce.MapFn) *Mapper {
	return &Mapper{
		master:         master,
		mapFn:          mapFn,
		reducerClients: util.NewRPCClients(),
		output:         make(chan [2]string, 10),
	}
}

// connectMaster creates RPC client and connects it to the master.
func (m *Mapper) connectMaster() error {
	conn, err := net.Dial("tcp", m.master)
	if err != nil {
		return err
	}
	m.masterClient = rpc.NewClient(conn)
	return nil
}

// mapData starts the map function and outputs data to a channel.
func (m *Mapper) mapData() error {
	if err := m.mapFn(m.partition, m.output); err != nil {
		return err
	}
	close(m.output)
	return nil
}

// shuffle sends the output from mapping to the right reducers.
func (m *Mapper) shuffle() error {
	for e := range m.output {
		h := fnv.New64a()
		h.Write([]byte(e[0]))
		hi := h.Sum64()
		p := int(hi % uint64(len(m.reducers)))
		client, err := m.reducerClients.Client(m.reducers[p])
		if err != nil {
			return err
		}
		if err := client.Call("Service.Element", &e, msg.Empty); err != nil {
			return err
		}
	}
	return nil
}

// Run registers the mapper with the master, getting its partition info, and starts reading data, mapping, and sending the results to the reducers.
func (m *Mapper) Run(masterAddrs string) error {
	if err := m.connectMaster(); err != nil {
		return err
	}
	logger.Print("Connected to master")
	info := new(msg.MapperInfo)
	if err := m.masterClient.Call("Service.NewMapper", msg.Empty, info); err != nil {
		return err
	}
	m.reducers = info.Reducers
	m.partition = info.Partition
	logger.Printf("Got partition %d", m.partition)
	// TODO: catch errors
	logger.Print("Starting to map and shuffle data")
	go m.mapData()
	if err := m.shuffle(); err != nil {
		return err
	}
	if err := m.masterClient.Call("Service.MapperFinished", msg.Empty, msg.Empty); err != nil {
		return err
	}
	logger.Print("Finished")
	return nil
}
