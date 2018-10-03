package msg

var (
	// Empty ...
	Empty = &EmptyMsg{}
)

// EmptyMsg is used in RPC for an empty message.
type EmptyMsg struct{}

// PartitionInfo ...
type PartitionInfo struct {
	Index int64
	Max   int64
}

// MapperInfo ...
type MapperInfo struct {
	PartInfo PartitionInfo
	Reducers map[int64]string
}
