package exec

import (
	"os/exec"
	"syscall"
)

func setParentDeathSignal(cmd *exec.Cmd, sig syscall.Signal) {
	if sig == 0 {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Pdeathsig = sig
}
