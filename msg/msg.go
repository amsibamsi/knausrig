package msg

import "net"

// EmptyMsg is used in RPC for an empty message.
type EmptyMsg struct{}

// PartitionInfo ...
type PartitionInfo struct {
	Index int64
	Max   int64
}

// MapperInfo ...
type MapperInfo struct {
	PartitionInfo PartitionInfo
	Reducers      map[int64]net.TCPAddr
}
