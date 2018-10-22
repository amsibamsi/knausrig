package main

import (
	"fmt"
	"strconv"

	"github.com/amsibamsi/knausrig"
)

func words(part, numPart int, out chan<- [2]string) error {
	for i := 0; i < 1e3; i++ {
		out <- [2]string{"word1", "1"}
		out <- [2]string{"word2", "2"}
	}
	return nil
}

func count(in <-chan [2]string, out chan<- [2]string) error {
	sums := make(map[string]int)
	for e := range in {
		word := e[0]
		amount, err := strconv.Atoi(e[1])
		if err != nil {
			return err
		}
		sums[word] = sums[word] + amount
	}
	for k, v := range sums {
		out <- [2]string{
			k,
			strconv.Itoa(v),
		}
	}
	return nil
}

func output(result <-chan [2]string) error {
	for r := range result {
		fmt.Printf("%s: %s\n", r[0], r[1])
	}
	return nil
}

func main() {
	mr := knausrig.Job{
		MapFn:    words,
		ReduceFn: count,
		OutputFn: output,
	}
	mr.Main()
}
