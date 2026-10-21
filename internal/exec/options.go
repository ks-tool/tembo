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
	"io"
	"syscall"
)

type Option func(*Command)

func SetSignal(sig syscall.Signal) Option {
	return func(cmd *Command) {
		cmd.sig = sig
	}
}

func SetCommand(name string, args ...string) Option {
	return func(cmd *Command) {
		cmd.name = name
		cmd.args = args
	}
}

func SetContext(ctx context.Context) Option {
	return func(cmd *Command) {
		cmd.ctx = ctx
	}
}

// SetOutput sends stdout and stderr of the command to w instead of os.Stdout.
func SetOutput(w io.Writer) Option {
	return func(cmd *Command) {
		cmd.out = w
	}
}

// SetErrOut sends stderr of the command to w instead of the writer of
// SetOutput: diagnostics on stderr are worth their own severity.
func SetErrOut(w io.Writer) Option {
	return func(cmd *Command) {
		cmd.errOut = w
	}
}
