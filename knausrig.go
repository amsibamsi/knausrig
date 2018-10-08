package knausrig

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/amsibamsi/knausrig/mapper"
	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/master"
	"github.com/amsibamsi/knausrig/reducer"
	"github.com/amsibamsi/knausrig/util"
)

var (
	operation = flag.String(
		"operation",
		"",
		"Exactly one of 'run', 'map', 'reduce' (required)",
	)
	mappersFilename = flag.String(
		"mappersFile",
		"mappers.txt",
		"Text file listing SSH destinations to use as mapper workers"+
			", one per line (operation: run)",
	)
	reducersFilename = flag.String(
		"reducersFile",
		"reducers.txt",
		"Text file listing SSH destinations to use as reducer workers"+
			", one per line (operation: run)",
	)
	masterAddrs = flag.String(
		"master",
		"",
		"Comma-delimited list of <net_address>:<port> to reach the master"+
			"(operation: map, reduce; required)",
	)
)

var (
	// Logger ...
	logger = log.New(os.Stderr, "", 0)
)

// run ...
func run() error {
	mappers, err := util.ReadLines(*mappersFilename)
	if err != nil {
		return err
	}
	reducers, err := util.ReadLines(*reducersFilename)
	if err != nil {
		return err
	}
	master := master.NewMaster(len(mappers), len(reducers))
	port, err := master.Run()
	if err != nil {
		return err
	}
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
	for i, reducer := range reducers {
		args := fmt.Sprintf(
			"-operation reduce -master %s",
			addrs,
		)
		id := "reducer-" + strconv.Itoa(i)
		if _, err := util.RunMeRemote(id, reducer, args); err != nil {
			return err
		}
	}
	for i, mapper := range mappers {
		args := fmt.Sprintf(
			"-operation map -master %s",
			addrs,
		)
		id := "mapper-" + strconv.Itoa(i)
		if _, err := util.RunMeRemote(id, mapper, args); err != nil {
			return err
		}
	}
	master.WaitFinish()
	return nil
}

// MapReduce ...
type MapReduce struct {
	MapFn    mapreduce.MapFn
	ReduceFn mapreduce.ReduceFn
	OutputFn mapreduce.OutputFn
}

// Main evaluates the operation argument and either starts a new job, or a
// mapper/reducer.
func (m *MapReduce) Main() {
	flag.Parse()
	switch *operation {
	case "run":
		if err := run(); err != nil {
			log.Fatal(err)
		}
	case "map":
		if err := mapper.NewMapper(m.MapFn).Run(*masterAddrs); err != nil {
			logger.Fatal(err)
		}
	case "reduce":
		if err := reducer.NewReducer(m.ReduceFn).Run(*masterAddrs); err != nil {
			logger.Fatal(err)
		}
	default:
		log.Fatal(fmt.Errorf("Unknown operation: %q", *operation))
	}
}
