package util

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
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
		scan := bufio.NewScanner(stdout)
		for scan.Scan() {
			log.Printf("%s: %s", id, scan.Text())
		}
		if err := scan.Err(); err != nil {
			log.Printf("%s: Error reading output: %s", id, err)
		}
	}()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	go func() {
		scan := bufio.NewScanner(stderr)
		for scan.Scan() {
			log.Printf("%s: %s", id, scan.Text())
		}
		if err := scan.Err(); err != nil {
			log.Printf("%s: Error reading error output: %s", id, err)
		}
	}()
	log.Printf("Remote %s: Command: %q", id, cmd.Args)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	if _, err := io.Copy(stdin, exe); err != nil {
		return nil, err
	}
	stdin.Close()
	return cmd, nil
}
