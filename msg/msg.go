package msg

var (
	// Empty ...
	Empty = &EmptyMsg{}
)

// EmptyMsg is used in RPC for an empty message.
type EmptyMsg struct{}

// MapperInfo ...
type MapperInfo struct {
	Partition int64
	Reducers  map[int64]string
}
