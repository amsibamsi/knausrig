package knausrig

import (
	"flag"
	"fmt"

	"github.com/amsibamsi/knausrig/mapper"
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

// MapFn ...
type MapFn func(int64, chan<- [2]string) error

// ReduceFn ...
type ReduceFn func([]string) (string, error)

// OutputFn ...
type OutputFn func(map[string]string) error

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
		addrs = addrs + ip.String() + ":" + port
	}
	for _, reducer := range reducers {
		args := fmt.Sprintf(
			"-operation reduce -master %s",
			addrs,
		)
		if err := util.RunMeRemote(reducer, args); err != nil {
			return err
		}
	}
	for _, mapper := range mappers {
		args := fmt.Sprintf(
			"-operation map -master %s",
			addrs,
		)
		if err := util.RunMeRemote(mapper, args); err != nil {
			return err
		}
	}
	master.WaitFinish()
	return nil
}

// MapReduce ...
type MapReduce struct {
	mapFn    MapFn
	reduceFn ReduceFn
	outputFn OutputFn
}

// Main evaluates the operation argument and either starts a new job, or a
// mapper/reducer.
func (m *MapReduce) Main() error {
	switch *operation {
	case "run":
		return run()
	case "map":
		mapper.NewMapper(mapFn).Run(*masterAddrs)
	case "reduce":
		reducer.NewReducer(reduceFn).Run(*masterAddrs)
	default:
		return fmt.Errorf("Unknown operation: %q", *operation)
	}
	return nil
}
