/*
 Copyright (c) 2025 Arenadata Softwer LLC.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package exec

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
)

const (
	PathEnvKey = "PATH"
)

type Env map[string]string

func (env Env) env() []string {
	var out []string
	for k, v := range env {
		out = append(out, fmt.Sprintf("%s=%s", k, v))
	}
	return out
}

type Command struct {
	env    Env
	out    io.Writer
	errOut io.Writer

	ctx    context.Context
	sig    syscall.Signal
	pdeath syscall.Signal
	name   string
	args   []string
}

func New(opts ...Option) *Command {
	cmd := &Command{
		env: make(Env),
		out: os.Stdout,
	}
	for _, opt := range opts {
		opt(cmd)
	}

	if _, ok := cmd.env[PathEnvKey]; !ok {
		p := os.Getenv(PathEnvKey)
		if len(p) > 0 {
			cmd.env[PathEnvKey] = p
		}
	}

	return cmd
}

func (c *Command) Command(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = c.out
	cmd.Stderr = c.errOut
	if cmd.Stderr == nil {
		cmd.Stderr = cmd.Stdout
	}
	cmd.Env = c.env.env()

	if c.sig > 0 {
		cmd.Cancel = func() error {
			return cmd.Process.Signal(c.sig)
		}
	}

	setParentDeathSignal(cmd, c.pdeath)

	return cmd
}

// OnParentDeath makes the kernel send sig to the process when this one dies,
// however it dies. It takes effect on Linux only, and for commands made after
// the call.
func (c *Command) OnParentDeath(sig syscall.Signal) {
	c.pdeath = sig
}

// Run runs the command given by SetCommand in the context given by SetContext,
// or in the background context when there is none.
func (c *Command) Run() error {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return c.Command(ctx, c.name, c.args...).Run()
}

func (c *Command) SetEnv(key, value string) {
	c.env[key] = value
}
