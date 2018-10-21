// Package mapper runs the map task of a MapReduce job.
package mapper

import (
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/msg"
	"github.com/amsibamsi/knausrig/util"
)

const (
	chanSize = 10
)

// Mapper reads input data and applies the user-defined mapping to it.
type Mapper struct {
	part           int
	numPart        int
	mapFn          mapreduce.MapFn
	master         string
	masterClient   *rpc.Client
	reducers       map[int]string
	reducerClients *util.RPCClients
	tasks          *sync.WaitGroup
}

// NewMapper returns a new mapper.
func NewMapper(part, numPart int, master string, mapFn mapreduce.MapFn) *Mapper {
	return &Mapper{
		part:           part,
		numPart:        numPart,
		mapFn:          mapFn,
		master:         master,
		reducerClients: util.NewRPCClients(),
		tasks:          &sync.WaitGroup{},
	}
}

func (m *Mapper) connectMaster() error {
	conn, err := net.Dial("tcp", m.master)
	if err != nil {
		return err
	}
	m.masterClient = rpc.NewClient(conn)
	log.Print("Connected to master")
	return nil
}

// mapShuffle starts the mapping/shuffling tasks in the background. The error
// channel will be closed without messages if there is no errors. Otherwise
// there will be events in it with value != 0.
func (m *Mapper) mapShuffle() error {
	out := make(chan [2]string, chanSize)
	// Input and map
	go func() {
		if err := m.mapFn(m.part, m.numPart, out); err != nil {
			log.Printf("Error in map function: %v", err)
		}
		close(out)
	}()
	// Shuffle to reducers
	count := 0
	for e := range out {
		count++
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
	log.Printf("Sent %d elements to %d reducers", count, len(m.reducers))
	return nil
}

// Run registers the mapper with the master, getting its partition info, and
// starts reading data, mapping, and sending the results to the reducers.
func (m *Mapper) Run() error {
	if err := m.connectMaster(); err != nil {
		return err
	}
	rmap := make(map[int]string)
	if err := m.masterClient.Call("Service.MapperStart", msg.Empty, rmap); err != nil {
		return err
	}
	m.reducers = rmap
	log.Print("Got reducers map")
	if err := m.mapShuffle(); err != nil {
		return err
	}
	if err := m.masterClient.Call("Service.MapperStop", msg.Empty, msg.Empty); err != nil {
		return err
	}
	log.Print("Finished")
	return nil
}

// Main runs the mapper and logs any errors.
func (m *Mapper) Main() {
	if err := m.Run(); err != nil {
		log.Fatal(err)
	}
}
