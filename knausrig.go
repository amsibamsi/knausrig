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
		"masterAddrs",
		"",
		"IP addresses to reach the master on"+
			"(operation: map, reduce; required)",
	)
	masterPort = flag.Int(
		"masterPort",
		-1,
		"IP port to reach the master on"+
			"(operation: map, reduce; required)",
	)
)

// Key ...
type Key int

// Value ...
type Value []byte

// Element ...
type Element struct {
	K Key
	V Value
}

// InputFn ...
type InputFn func(part, maxPart int64, out chan<- Element) error

// MapFn ...
type MapFn func(e Element) (Element, error)

// ReduceFn ...
type ReduceFn func(e Element) (Element, error)

// OutputFn ...
type OutputFn func(map[Key]Value) error

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
		if addrs == "" {
			addrs = ip.String()
		} else {
			addrs = addrs + "," + ip.String()
		}
	}
	for _, reducer := range reducers {
		args := fmt.Sprintf(
			"-operation reduce -masterPort %d -masterAddrs %s",
			port,
			addrs,
		)
		if err := util.RunMeRemote(reducer, args); err != nil {
			return err
		}
	}
	for _, mapper := range mappers {
		args := fmt.Sprintf(
			"-operation map -masterPort %d -masterAddrs %s",
			port,
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
	inputFn  InputFn
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
		mapper.NewMapper(inputFn, mapFn).Run(*masterAddrs, *masterPort)
	case "reduce":
		reducer.NewReducer(reduceFn).Run(*masterAddrs, *masterPort)
	default:
		return fmt.Errorf("Unknown operation: %q", *operation)
	}
	return nil
}
