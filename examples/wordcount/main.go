package main

import (
	"fmt"
	"strconv"

	"github.com/amsibamsi/knausrig"
)

func words(part int64, out chan<- [2]string) error {
	for i := 0; int64(i) <= part; i++ {
		for j := 0; j < i+1; j++ {
			out <- [2]string{
				fmt.Sprintf("word-%d", i),
				"1",
			}
		}
	}
	return nil
}

func count(_ string, elements []string) (string, error) {
	sum := 0
	for _, e := range elements {
		i, err := strconv.Atoi(e)
		if err != nil {
			return "", err
		}
		sum = sum + i
	}
	return strconv.Itoa(sum), nil
}

func output(result map[string]string) error {
	fmt.Printf("Result: $%v\n", result)
	return nil
}

func main() {
	mr := knausrig.MapReduce{
		MapFn:    words,
		ReduceFn: count,
		OutputFn: output,
	}
	mr.Main()
}
