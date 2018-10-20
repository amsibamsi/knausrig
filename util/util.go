// Package util provides utility functions.
package util

import (
	"net"
	"net/rpc"
)

// RPCClients lazily initializes and returns RPC clients.
type RPCClients struct {
	Clients map[string]*rpc.Client
}

// NewRPCClients returns a new RPCClients struct with Clients initialized to an
// empty map.
func NewRPCClients() *RPCClients {
	return &RPCClients{make(map[string]*rpc.Client)}
}

// Client returns the existing RPC client for the specified address, or
// initializes a new one and returns it.
func (r *RPCClients) Client(addr string) (*rpc.Client, error) {
	if client, ok := r.Clients[addr]; ok {
		return client, nil
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	client := rpc.NewClient(conn)
	r.Clients[addr] = client
	return client, nil
}
