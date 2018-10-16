// Package util provides utility functions.
package util

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
)

// ReadLines reads the entire file and returns all lines that are not just
// whitespace.
func ReadLines(filename string) ([]string, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	split := strings.Split(string(content), "\n")
	var lines []string
	for _, s := range split {
		l := strings.TrimSpace(s)
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines, nil
}

// LocalIPs returns the list of local non-loopback IP addresses.
func LocalIPs() ([]net.IP, error) {
	var ips []net.IP
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		switch a := addr.(type) {
		case *net.IPNet:
			if !a.IP.IsLoopback() {
				ips = append(ips, a.IP)
			}
		}
	}
	return ips, nil
}

const (
	// ScriptTmpl is template to use for running current executable over
	// SSH remotely, should be expaned with Printf to add arguments for
	// binary executed remotely.
	// Trying to remove the binary after execution in any case, no leftovers.
	ScriptTmpl = `sh -c 'bin=$(mktemp) && cat - > $bin && chmod +x $bin` +
		` && $bin %s; rm $bin'`
)

// RunMeRemote copies the binary of the current process to a remote destination
// and tries to execute it there via SSH. The supplied args are passed to the
// executable when it's run remotely. The binary is copied to a temp file on
// the remote host, and removed again after execution. The dst argument can be
// any valid destination understood by the 'ssh' client executable.
// Does not wait for the command to finish, returns the command.
func RunMeRemote(id, dst, args string) (*exec.Cmd, error) {
	cmd := exec.Command("ssh", dst, fmt.Sprintf(ScriptTmpl, args))
	exeFilename, err := os.Executable()
	exe, err := os.Open(exeFilename)
	if err != nil {
		return nil, err
	}
	defer exe.Close()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	go func() {
		logger := log.New(os.Stderr, "["+id+"] ", 0)
		scan := bufio.NewScanner(stdout)
		for scan.Scan() {
			logger.Printf("%s: %s", id, scan.Text())
		}
		if err := scan.Err(); err != nil {
			logger.Printf("%s: Error reading output: %s", id, err)
		}
	}()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	go func() {
		logger := log.New(os.Stderr, "["+id+"] ", 0)
		scan := bufio.NewScanner(stderr)
		for scan.Scan() {
			logger.Printf("%s: %s", id, scan.Text())
		}
		if err := scan.Err(); err != nil {
			logger.Printf("%s: Error reading error output: %s", id, err)
		}
	}()
	log.Printf("Remote %s: running command: %q", id, cmd.Args)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	if _, err := io.Copy(stdin, exe); err != nil {
		return nil, err
	}
	stdin.Close()
	return cmd, nil
}

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
