package job

import (
	"flag"
	"log"
)

var (
	inputFilename = flag.String(
		"sourceFile",
		"",
		"File containing the entire input data for the MapReduce",
	)
	mappersFilename = flag.String(
		"mappersFile",
		"",
		"Text file listing hostnames to use as mapper workers, one per line",
	)
	reducersFilename = flag.String(
		"reducersFile",
		"",
		"Text file listing hostnames to use as reducer workers, one per line",
	)
)

// Main runs a new job.
func Main() {
	log.Print("New job")
}
