// Package knausrig provides a very cheap example of a MapReduce job, running
// distributed processes via SSH. To start create a new MapReduce with
// customized functions for mapping (includes inputting), reducing and
// outputing data, then call Main() on it to start the process.
package knausrig

import (
	"flag"
	"fmt"
	"log"

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
	masterAddr = flag.String(
		"master",
		"",
		"<address>:<port> of master server"+
			" (required; modes: map, reduce)",
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
	var err error
	flag.Parse()
	switch *mode {
	case "master":
		master, err := master.NewMaster(j.OutputFn)
		if err != nil {
			log.Fatal(err)
		}
		err = master.Run()
		if err != nil {
			log.Fatal(err)
		}
	case "map":
		err = mapper.NewMapper(*masterAddr, j.MapFn).Run()
		if err != nil {
			log.Fatal(err)
		}
	case "reduce":
		err = reducer.NewReducer(
			*listenAddr,
			*masterAddr,
			*j.ReduceFn,
		).Run()
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal(fmt.Errorf("Unknown mode: %q", *mode))
	}
}
