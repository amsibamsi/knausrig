package mapreduce

// MapFn ...
type MapFn func(int64, chan<- [2]string) error

// ReduceFn ...
type ReduceFn func(string, []string) (string, error)

// OutputFn ...
type OutputFn func(map[string]string) error
