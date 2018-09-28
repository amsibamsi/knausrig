package util

import (
	"io/ioutil"
	"strings"
)

// ReadLines reads the entire file and returns its lines as a slice of strings.
func ReadLines(filename string) ([]string, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(content), "\n"), nil
}
