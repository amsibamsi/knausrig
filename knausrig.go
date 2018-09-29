package knausrig

import (
	"flag"
	"fmt"
	"log"

	"github.com/amsibamsi/knausrig/mapper"
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
		"./mappers.txt",
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

// run ...
func run() error {
	ips, err := util.LocalIPs()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Local IPs: %v", ips)
	return nil
}

// Main evaluates the operation argument and either starts a new job, or a
// mapper/reducer.
func Main() error {
	switch *operation {
	case "run":
		return run()
	case "map":
		mapper.Main()
	case "reduce":
		reducer.Main()
	default:
		return fmt.Errorf("Unknown operation: %q", *operation)
	}
	return nil
}
