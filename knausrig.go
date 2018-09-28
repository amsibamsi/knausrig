package knausrig

import (
	"flag"
	"log"

	"github.com/amsibamsi/kmr/job"
	"github.com/amsibamsi/kmr/mapper"
	"github.com/amsibamsi/kmr/reducer"
)

var (
	operation = flag.String(
		"operation",
		"",
		"Exactly one of 'job', 'map', 'reduce'.",
	)
)

// Main evaluates the operation argument and either starts a new job, or a
// mapper/reducer.
func Main() {
	switch *operation {
	case "job":
		job.Main()
	case "map":
		mapper.Main()
	case "reduce":
		reducer.Main()
	default:
		log.Fatalf("Unknown operation: %q", *operation)
	}
}
