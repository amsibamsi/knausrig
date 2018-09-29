package util

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
)

// ReadLines reads the entire file and returns its lines as a list of strings.
func ReadLines(filename string) ([]string, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(content), "\n"), nil
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
	ScriptTmpl = `sh -c 'bin=$(mktemp) && cat - > $bin && chmod +x $bin ` +
		`&& $bin %s; binexit=$?; rm $bin; rmexit=$?; ` +
		`if [ "$binexit" = 0 ]; then exit $rmexit; else exit $binexit; fi'`
)

// RunMeRemote copies the binary of the current process to a remote destination
// and tries to execute it there via SSH. The supplied args are passed to the
// executable when it's run remotely. The binary is copied to a temp file on
// the remote host, and removed again after successfull execution. The dst
// argument can be any valid destination understood by the 'ssh' client
// executable.
func RunMeRemote(dst string, args string) error {
	cmd := exec.Command("ssh", dst, fmt.Sprintf(ScriptTmpl, args))
	log.Printf(
		"Starting remote command %q on destination %q",
		cmd.Args,
		dst,
	)
	exeFilename, err := os.Executable()
	exe, err := os.Open(exeFilename)
	defer exe.Close()
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	if _, err := io.Copy(stdin, exe); err != nil {
		return err
	}
	stdin.Close()
	err = cmd.Wait()
	for _, line := range strings.Split(stdout.String(), "\n") {
		log.Printf("Remote command out: %q", line)
	}
	for _, line := range strings.Split(stderr.String(), "\n") {
		log.Printf("Remote command err: %q", line)
	}
	if err != nil {
		return err
	}
	return nil
}
