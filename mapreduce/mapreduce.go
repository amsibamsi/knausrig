// Package mapreduce contains the basic types for a MapReduce process, to be
// implemented by a custom job.
//
// TODO: Make this more generic by not relying on specific string type (e.g.
// use interfaces?).
package mapreduce

// MapFn receives the partition and total number of partitions as input, and
// should output (key,value) pairs of type string to the output channel.
type MapFn func(int, int, chan<- [2]string) error

// ReduceFn receives (key,value) pairs of type string on the first channel and
// should output reduced pairs to the second channel. It will receive multiple
// elements for the same key. If it receives values for a specific key, it will
// receive all the values for this key. It must not output multiple pairs for
// the same key.
type ReduceFn func(<-chan [2]string, chan<- [2]string) error

// OutputFn receives (key,value) pairs of type string, one pair for each key
// with the reduced value. It's supposed to output/write the final result of
// the MapReduce job.
type OutputFn func(<-chan [2]string) error
