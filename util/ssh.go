package util

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
)

const (
	// ScriptTmpl is template to use for running current executable over
	// SSH remotely, should e expaned with Printf to add arguments for
	// binary executed remotely.
	ScriptTmpl = "sh -c 'bin=$(mktemp); cat - > $bin; chmod +x $bin; $bin %s; rm $bin'"
)

// RunMeRemote ...
func RunMeRemote(host string, args string) error {
	cmd := exec.Command("ssh", host, fmt.Sprintf(ScriptTmpl, args))
	log.Printf(
		"Starting remote command %q on host %q",
		cmd.Args,
		host,
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
	if err := cmd.Wait(); err != nil {
		return err
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		log.Printf("Remote command out: %q", line)
	}
	for _, line := range strings.Split(stderr.String(), "\n") {
		log.Printf("Remote command err: %q", line)
	}
	return nil
}
