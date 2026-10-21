//go:build !linux

package exec

import (
	"os/exec"
	"syscall"
)

func setParentDeathSignal(*exec.Cmd, syscall.Signal) {}
