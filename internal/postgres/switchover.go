package postgres

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/spf13/cobra"
)

var switchoverAt string

var switchoverCmd = &cobra.Command{
	Use:   "switchover node",
	Short: "Move the primary to a standby without losing transactions",
	Long: `Move the primary to a running standby. The primary stops cleanly, which
sends all its WAL to the standbys, and the standby is promoted once it has
received it. Meanwhile clients are refused. The command waits for the outcome.
With --at the switchover is scheduled instead, and the command answers at once.`,
	Args: cobra.ExactArgs(1),
	RunE: switchoverCmdRun,
}

var failoverCmd = &cobra.Command{
	Use:   "failover node",
	Short: "Promote a standby while the primary is down",
	Long: `Promote a running standby without waiting for the old primary, which a
switchover refuses to do while the primary is unreachable. Use when the primary
is down for good — e.g. to fail over manually while the cluster is paused.`,
	Args: cobra.ExactArgs(1),
	RunE: failoverCmdRun,
}

func init() {
	switchoverCmd.Flags().StringVar(&switchoverAt, "at", "", "schedule the switchover, RFC3339")
	rootCmd.AddCommand(switchoverCmd, failoverCmd)
}

func switchoverCmdRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(cmd.Context(), 90*time.Second)
	defer cancel()
	var body []byte
	if switchoverAt != "" {
		body = []byte(switchoverAt)
	}
	return call(ctx, http.MethodPut, "/switchover/"+url.PathEscape(args[0]), body)
}

func failoverCmdRun(cmd *cobra.Command, args []string) error {
	return call(cmd.Context(), http.MethodPost, "/failover/"+url.PathEscape(args[0]), nil)
}
