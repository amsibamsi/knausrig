package util

import (
	"os/exec"
)

// RemoteCmd ...
func RemoteCmd(host, cmd string, args ...string) (string, error) {
	sshArgs := append([]string{host, cmd}, args...)
	c := exec.Command("ssh", sshArgs...)
	out, err := c.Output()
	return string(out), err
}

// RunBinRemote ...
func RunBinRemote(bin, host string, args ...string) (string, error) {
}
