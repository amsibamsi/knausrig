package msg

var (
	// Empty ...
	Empty = &EmptyMsg{}
)

// EmptyMsg is used in RPC for an empty message.
type EmptyMsg struct{}

// MapperInfo ...
type MapperInfo struct {
	Partition int
	Reducers  map[int]string
}
