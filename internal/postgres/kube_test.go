package postgres

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	appconfig "github.com/ks-tool/app-config/go"
	"github.com/spf13/pflag"
)

func TestRunConfigLoadPriority(t *testing.T) {
	t.Setenv("TEMBO_PG_PORT", "5444")
	t.Setenv("TEMBO_LISTEN", ":15432")
	cfg := runConfig{Listen: ":5432", ListenRO: ":5434", Retention: 8 << 20, PGPort: 5433}
	fs := pflag.NewFlagSet("run", pflag.ContinueOnError)
	appconfig.BindFlags(fs, "", &cfg)
	if err := fs.Parse([]string{"--pg-port", "5555"}); err != nil {
		t.Fatal(err)
	}
	// stands in for the --config file and the Kubernetes objects
	kv := map[string]string{"TEMBO_PG_PORT": "5400", "TEMBO_PG_DATA": "/data", "TEMBO_LISTEN": ":25432"}
	err := appconfig.Load(&cfg, appconfig.KV(kv), appconfig.Env(), appconfig.Flags(fs),
		appconfig.Options(appconfig.EnvOptions{Prefix: "TEMBO_"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PGPort != 5555 {
		t.Errorf("PGPort = %d, want the flag's 5555 over env and the sources", cfg.PGPort)
	}
	if cfg.Listen != ":15432" {
		t.Errorf("Listen = %q, want the env's :15432 over the sources", cfg.Listen)
	}
	if cfg.PGData != "/data" {
		t.Errorf("PGData = %q, want the sources' /data over the default", cfg.PGData)
	}
	if cfg.ListenRO != ":5434" {
		t.Errorf("ListenRO = %q, want the default :5434", cfg.ListenRO)
	}
}

func TestKubeConfigPatch(t *testing.T) {
	patch, err := kubeConfigPatch(`{"work_mem":"64MB","shared_buffers":null}`)
	if err != nil {
		t.Fatal(err)
	}
	if *patch["work_mem"] != "64MB" || patch["shared_buffers"] != nil {
		t.Fatalf("patch: %v", patch)
	}
	if _, err := kubeConfigPatch(`{"synchronous_standby_names":"FIRST 1 (a)"}`); err == nil {
		t.Error("a blocked parameter: want an error")
	}
	if _, err := kubeConfigPatch(`{"archive_command":"curl http://x/%p"}`); err == nil {
		t.Error("a command parameter: want an error")
	}
	if _, err := kubeConfigPatch(`{"bad name!":"1"}`); err == nil {
		t.Error("a bad parameter name: want an error")
	}
}

func TestKubeSettingsPatch(t *testing.T) {
	normalized, err := kubeSettingsPatch(`{
		"synchronous_mode":"quorum",
		"synchronous_node_count":"2",
		"max-lag-on-failover":"16MB",
		"paused":true,
		"hba":["host all all 10.0.0.0/24 scram-sha-256"]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	if normalized["synchronous_mode"] != "quorum" {
		t.Errorf("synchronous_mode: %v", normalized["synchronous_mode"])
	}
	if normalized["synchronous_node_count"] != uint64(2) {
		t.Errorf("synchronous_node_count: %v (%T)", normalized["synchronous_node_count"], normalized["synchronous_node_count"])
	}
	if normalized["max-lag-on-failover"] != uint64(16<<20) {
		t.Errorf("max-lag-on-failover: %v", normalized["max-lag-on-failover"])
	}
	if normalized["paused"] != true {
		t.Errorf("paused: %v", normalized["paused"])
	}
	hba, ok := normalized["hba"].([]any)
	if !ok || len(hba) != 1 || hba[0] != "host all all 10.0.0.0/24 scram-sha-256" {
		t.Errorf("hba: %v", normalized["hba"])
	}

	normalized, err = kubeSettingsPatch(`{"synchronous_mode":null}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := normalized["synchronous_mode"]; ok {
		t.Errorf("a null must drop the setting: %v", normalized)
	}

	if _, err := kubeSettingsPatch(`{"nope":"1"}`); err == nil {
		t.Error("an unknown setting: want an error")
	}
	if _, err := kubeSettingsPatch(`{"synchronous_mode":"sometimes"}`); err == nil {
		t.Error("a bad value: want an error")
	}
}

func TestManagedForbidsPatches(t *testing.T) {
	mux := http.NewServeMux()
	configHandlers(mux, nil, nil, nil, true)
	settingsHandlers(mux, nil, nil, true)
	for _, path := range []string{"/config", "/settings"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, path, strings.NewReader("{}"))
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("PATCH %s in Kubernetes mode: got %d, want 403", path, rec.Code)
		}
	}
}
