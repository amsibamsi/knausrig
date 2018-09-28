package job

import (
	"flag"
	"log"
	"net"
	"net/rpc"
)

var (
	inputFilename = flag.String(
		"sourceFile",
		"",
		"File containing the entire input data for the MapReduce",
	)
	mappersFilename = flag.String(
		"mappersFile",
		"",
		"Text file listing hostnames to use as mapper workers, one per line",
	)
	reducersFilename = flag.String(
		"reducersFile",
		"",
		"Text file listing hostnames to use as reducer workers, one per line",
	)
)

// Server ...
type Server struct{}

// Message ...
type Message struct {
	Content string
}

// Ping ...
func (s *Server) Ping(req, resp *Message) error {
	resp.Content = req.Content
	return nil
}

// Main runs a new job.
func Main() {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	addr := listener.Addr().String()
	log.Printf("Listening on %q", addr)
	rpc.Register(&Server{})
	go func() { rpc.Accept(listener) }()
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	var req, resp Message
	req.Content = "ping"
	client.Call("Server.Ping", &req, &resp)
	log.Printf("Pong: %q", resp.Content)
}
