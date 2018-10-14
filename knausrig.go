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
	mode = flag.String(
		"mode",
		"master",
		"Exactly one of 'master', 'map', 'reduce'",
	)
	config = flag.String(
		"config",
		"./config.json",
		"Configuration file for master (modes: master)",
	)
	masterAddr = flag.String(
		"master",
		"",
		"<address>:<port> of master server (required; modes: map, reduce)",
	)
)

var (
	// Logger ...
	logger = log.New(os.Stderr, "", 0)
)

// run ...
func run(outputFn mapreduce.OutputFn) error {
	master := master.NewMaster(len(mappers), len(reducers), outputFn)
	err := master.Run()
	if err != nil {
		return err
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
		if err := run(m.OutputFn); err != nil {
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
