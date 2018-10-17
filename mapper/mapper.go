// Package mapper contains the mapper task of a MapReduce job.
package mapper

import (
	"errors"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"

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
}

// NewMapper returns a new mapper.
func NewMapper(master string, mapFn mapreduce.MapFn) *Mapper {
	return &Mapper{
		master:         master,
		mapFn:          mapFn,
		reducerClients: util.NewRPCClients(),
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
			logger.Printf("Sent element with key %q and value %q to reducer %d", e[0], e[1], p)
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
	logger.Print("Connected to master")
	info := new(msg.MapperInfo)
	if err := m.masterClient.Call("Service.NewMapper", msg.Empty, info); err != nil {
		return err
	}
	m.reducers = info.Reducers
	m.partition = info.Partition
	logger.Printf("Got partition %d", m.partition)
	logger.Print("Starting to map and shuffle data")
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
	logger.Print("Finished")
	return nil
}
