package main

import (
	"fmt"
	"strconv"

	"github.com/amsibamsi/knausrig"
)

func words(part int, out chan<- [2]string) error {
	out <- [2]string{"abc", "3"}
	out <- [2]string{"def", "2"}
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
	mr := knausrig.Job{
		MapFn:    words,
		ReduceFn: count,
		OutputFn: output,
	}
	mr.Main()
}
