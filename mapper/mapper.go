// Package mapper runs the map task of a MapReduce job.
package mapper

import (
	"errors"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/msg"
	"github.com/amsibamsi/knausrig/util"
)

// A mapper reads input data and applies the user-defined mapping to it.
type Mapper struct {
	part           int
	numPart        int
	log            *log.Logger
	master         string
	mapFn          mapreduce.MapFn
	masterClient   *rpc.Client
	reducers       map[int]string
	reducerClients *util.RPCClients
	tasks          *sync.WaitGroup
}

// NewMapper returns a new mapper.
func NewMapper(part, numPart int, master string, mapFn mapreduce.MapFn) *Mapper {
	id := "[mapper-" + strconv.Itoa(part) + "]"
	return &Mapper{
		part:           part,
		numPart:        numPart,
		log:            log.New(os.Stderr, id, log.LstdFlags),
		master:         master,
		mapFn:          mapFn,
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
	return nil
}

// mapShuffle starts the mapping/shuffling tasks in the background. The error
// channel will be closed without messages if there is no errors. Otherwise
// there will be events in it with value != 0.
func (m *Mapper) mapShuffle(errs chan<- int) {
	out := make(chan [2]string, 10)
	// Input and map
	go func() {
		if err := m.mapFn(m.partition, out); err != nil {
			errs <- 1
			return
		}
		close(out)
	}()
	// Shuffle to reducers
	go func() {
		for e := range out {
			h := fnv.New64a()
			h.Write([]byte(e[0]))
			hi := h.Sum64()
			p := int(hi % uint64(len(m.reducers)))
			client, err := m.reducerClients.Client(m.reducers[p])
			if err != nil {
				errs <- 1
				return
			}
			if err := client.Call("Service.Element", &e, msg.Empty); err != nil {
				errs <- 1
				return
			}
			m.log.Printf("Sent element with key %q and value %q to reducer %d", e[0], e[1], p)
		}
		close(errs)
	}()
}

// Run registers the mapper with the master, getting its partition info, and
// starts reading data, mapping, and sending the results to the reducers.
func (m *Mapper) Run() error {
	if err := m.connectMaster(); err != nil {
		return err
	}
	m.log.Print("Connected to master")
	info := new(msg.MapperInfo)
	if err := m.masterClient.Call("Service.NewMapper", msg.Empty, info); err != nil {
		return err
	}
	m.reducers = info.Reducers
	m.partition = info.Partition
	m.log.Printf("Got partition %d", m.partition)
	m.log.Print("Starting to map and shuffle data")
	errs := make(chan int)
	m.mapShuffle(errs)
	select {
	case e := <-errs:
		if e != 0 {
			return errors.New("Aborting due to failures")
		}
	case <-time.After(time.Hour):
		return errors.New("Map/shuffle timed out")
	}
	if err := m.masterClient.Call("Service.MapperFinished", msg.Empty, msg.Empty); err != nil {
		return err
	}
	m.log.Print("Finished")
	return nil
}
