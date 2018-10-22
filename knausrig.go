// Package knausrig provides a very cheap example of a MapReduce job, running
// distributed processes via SSH. To start create a new MapReduce with
// customized functions for mapping (includes inputting), reducing and
// outputing data, then call Main() on it to start the process.
package knausrig

import (
	"flag"
	"fmt"
	"log"

	"github.com/amsibamsi/knausrig/cfg"
	"github.com/amsibamsi/knausrig/mapper"
	"github.com/amsibamsi/knausrig/mapreduce"
	"github.com/amsibamsi/knausrig/master"
	"github.com/amsibamsi/knausrig/reducer"
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
		"<address>:<port> of master server"+
			" (required; modes: map, reduce)",
	)
	part = flag.Int(
		"part",
		0,
		"Partition assigned to this mapper/reducer"+
			" (required; modes: map, reduce)",
	)
	numPart = flag.Int(
		"numPart",
		0,
		"Number of partitions (number of mappers)"+
			" (required; modes: map)",
	)
	listenAddr = flag.String(
		"listen",
		"",
		"<address>:<port> for local listening"+
			" (required; modes: reduce)",
	)
)

// Job holds the 3 customized functions for the actual computations done by the
// MapReduce job.
type Job struct {
	MapFn    mapreduce.MapFn
	ReduceFn mapreduce.ReduceFn
	OutputFn mapreduce.OutputFn
}

// Main evaluates the evaluates the mode and either starts a new job, or a new
// mapper/reducer.
func (j *Job) Main() {
	flag.Parse()
	switch *mode {
	case "master":
		config, err := cfg.FromFile(*config)
		if err != nil {
			log.Fatal(err)
		}
		master.NewMaster(
			config,
			j.OutputFn,
		).Main()
	case "map":
		mapper.NewMapper(
			*part,
			*numPart,
			*masterAddr,
			j.MapFn,
		).Main()
	case "reduce":
		reducer.NewReducer(
			*listenAddr,
			*masterAddr,
			j.ReduceFn,
		).Main()
	default:
		log.Fatal(fmt.Errorf("Unknown mode: %q", *mode))
	}
}
