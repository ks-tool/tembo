package postgres

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

var membersCmd = &cobra.Command{
	Use:   "members",
	Short: "Show or change the cluster membership",
}

var membersListCmd = &cobra.Command{
	Use:  "list",
	Args: cobra.NoArgs,
	RunE: membersListCmdRun,
}

var membersAddCmd = &cobra.Command{
	Use:  "add id=host:port",
	Args: cobra.ExactArgs(1),
	RunE: membersAddCmdRun,
}

var membersRemoveCmd = &cobra.Command{
	Use:  "remove id",
	Args: cobra.ExactArgs(1),
	RunE: membersRemoveCmdRun,
}

func init() {
	membersCmd.AddCommand(membersListCmd, membersAddCmd, membersRemoveCmd)
	rootCmd.AddCommand(membersCmd)
}

func membersListCmdRun(cmd *cobra.Command, _ []string) error {
	return call(cmd.Context(), http.MethodGet, "/members", nil)
}

func membersAddCmdRun(cmd *cobra.Command, args []string) error {
	id, addr, ok := strings.Cut(args[0], "=")
	if !ok || addr == "" {
		return fmt.Errorf("bad member %q, want id=host:port", args[0])
	}
	if !nodeID.MatchString(id) {
		return fmt.Errorf("bad member id %q, want [a-z0-9_] only: it names a replication slot", id)
	}
	return call(cmd.Context(), http.MethodPut, "/members/"+id, []byte(addr))
}

func membersRemoveCmdRun(cmd *cobra.Command, args []string) error {
	return call(cmd.Context(), http.MethodDelete, "/members/"+args[0], nil)
}
