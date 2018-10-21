// Package msg contains basic types for messaging between RPC services.
package msg

var (
	// Empty var is a global shortcut for an empty message.
	Empty = &EmptyMsg{}
)

// EmptyMsg is used in RPC for an empty message.
type EmptyMsg struct{}
