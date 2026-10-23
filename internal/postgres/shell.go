package postgres

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
)

// shell.go is the client of POST /shell. psql runs on the node, in the
// agent's container, and logs in there as the Postgres role given here; this
// command only carries the terminal to it. The input goes up as the request
// body, the output comes back as it is written. Nothing needs installing on
// the machine the command runs on.

var shellCmd = &cobra.Command{
	Use:     "shell [--db name] [--user role]",
	Aliases: []string{"psql"},
	Short:   "Open psql on the node's Postgres as a role of the cluster: tembo shell --user app",
	Args:    cobra.NoArgs,
	RunE:    shellCmdRun,
}

var shellDB, shellUser string

func init() {
	shellCmd.Flags().StringVar(&shellDB, "db", "", "database to connect to, default postgres")
	shellCmd.Flags().StringVar(&shellUser, "user", "", "Postgres role to log in as (PGUSER)")
	rootCmd.AddCommand(shellCmd)
}

func shellCmdRun(cmd *cobra.Command, _ []string) error {
	path := "/shell"
	if shellDB != "" {
		path += "?db=" + url.QueryEscape(shellDB)
	}
	hdr := http.Header{}
	// Over Kerberos the agent logs in as the user itself, so no role or
	// password is asked for. Otherwise the session logs in as the role given.
	if !cliKrb5 && cliKrb5Keytab == "" {
		user := shellUser
		if user == "" {
			user = os.Getenv("PGUSER")
		}
		if user == "" {
			return errors.New("a Postgres role is required: --user or PGUSER, or --krb5 to log in as yourself")
		}
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = promptPassword(user)
		}
		if err := plainPassword(agentURL(), password); err != nil {
			return err
		}
		hdr.Set("X-Tembo-Pg-User", user)
		hdr.Set("X-Tembo-Pg-Password", password)
	}
	// No deadline: a session lasts as long as the user keeps it open.
	res, err := send(cmd.Context(), http.MethodPost, path, os.Stdin, hdr)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode >= http.StatusMultipleChoices {
		data, _ := io.ReadAll(res.Body)
		return fmt.Errorf("%s: %s", res.Status, strings.TrimSpace(string(data)))
	}
	_, err = io.Copy(os.Stdout, res.Body)
	return err
}

// promptPassword asks for the role's password on the terminal, with echo off.
// Without a terminal it returns "": the session then goes without one, which
// works where pg_hba.conf asks for no password.
func promptPassword(role string) string {
	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return ""
	}
	defer func() { _ = tty.Close() }()
	_, _ = fmt.Fprintf(tty, "Password for %s: ", role)
	off := exec.Command("stty", "-echo")
	off.Stdin = tty
	_ = off.Run()
	defer func() {
		on := exec.Command("stty", "echo")
		on.Stdin = tty
		_ = on.Run()
		_, _ = fmt.Fprintln(tty)
	}()
	line, _ := bufio.NewReader(tty).ReadString('\n')
	return strings.TrimRight(line, "\r\n")
}

// plainPassword refuses to send a password over plain HTTP to another host:
// the header would travel in clear. Loopback and https are fine.
func plainPassword(rawURL, password string) error {
	if password == "" {
		return nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	if u.Scheme == "https" {
		return nil
	}
	host := u.Hostname()
	if host == "localhost" || (net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback()) {
		return nil
	}
	return errors.New("the Postgres password would travel in clear over http: use https (--tls-*) or run the command on the node")
}
