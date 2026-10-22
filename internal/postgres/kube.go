package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	appconfig "github.com/ks-tool/app-config/go"
	"github.com/ks-tool/app-config/go/k8s"
	"github.com/ks-tool/tembo/internal/agent"
	"github.com/ks-tool/tembo/internal/store"

	"github.com/rs/zerolog/log"
)

// rcSources builds the sources appconfig loads rc from, in growing priority:
// the --config file, the Kubernetes objects of --k8s, the environment and the
// command line. The Kubernetes client is nil when --k8s is not given. The
// file and the URIs come from a first load of the environment and the command
// line alone: they decide what the other sources are.
func rcSources(ctx context.Context, fs *appconfig.FlagSet) (*k8s.Client, []appconfig.Source, error) {
	if err := appconfig.Load(&rc, appconfig.Env(), appconfig.Flags(fs), appconfig.Options(prefixOpts)); err != nil {
		return nil, nil, err
	}

	var sources []appconfig.Source
	if rc.Config != "" {
		sources = append(sources, appconfig.File(rc.Config))
	}

	var kube *k8s.Client
	if len(rc.K8s) > 0 {
		var err error
		kube, err = kubeClient()
		if err != nil {
			return nil, nil, err
		}
		src, err := kube.Source(ctx, rc.K8s...)
		if err != nil {
			return nil, nil, err
		}
		sources = append(sources, src)
	}
	sources = append(sources, appconfig.Env(), appconfig.Flags(fs), appconfig.Options(prefixOpts))

	return kube, sources, nil
}

// kubeClient returns a Kubernetes client: in-cluster inside a pod, the
// kubeconfig of $KUBECONFIG outside, e.g. in development.
func kubeClient() (*k8s.Client, error) {
	c, err := k8s.InCluster()
	if err == nil {
		return c, nil
	}
	if path := os.Getenv("KUBECONFIG"); path != "" {
		return k8s.Kubeconfig(path)
	}
	return nil, err
}

// kubeWatcher keeps the cluster's configuration in line with the Kubernetes
// objects of --k8s: their config.json and settings.json are the desired
// postgresql.conf settings and agent settings. Only the raft leader writes;
// every node watches, so a new leader picks the state up.
type kubeWatcher struct {
	srv  *agent.Server
	st   *store.Store
	pg   *Instance
	mu   sync.Mutex
	last map[string]string // the newest set the informers reported
}

func newKubeWatcher(srv *agent.Server, st *store.Store, pg *Instance) *kubeWatcher {
	return &kubeWatcher{srv: srv, st: st, pg: pg}
}

// apply proposes the newest reported set, if any. It is the OnLeaderChange
// callback too: a change that arrived while this node was not the leader is
// not replayed by the informers, so gaining leadership re-applies it.
func (w *kubeWatcher) apply(ctx context.Context) {
	w.mu.Lock()
	kv := w.last
	w.mu.Unlock()
	if kv != nil {
		kubeApply(ctx, w.srv, w.st, w.pg, kv)
	}
}

// watch feeds the informers' changes in: onChange fires once they have
// synced and on every change of the ConfigMaps and Secrets of --k8s, so no
// re-apply on a timer is needed.
func (w *kubeWatcher) watch(ctx context.Context, kube *k8s.Client) {
	err := kube.Watch(ctx, rc.K8s, func(kv map[string]string) {
		w.mu.Lock()
		w.last = kv
		w.mu.Unlock()
		w.apply(ctx)
	})
	if err != nil && ctx.Err() == nil {
		log.Error().Err(err).Msg("kubernetes watch")
	}
}

func kubeApply(ctx context.Context, srv *agent.Server, st *store.Store, pg *Instance, kv map[string]string) {
	if !srv.IsLeader() {
		return
	}
	if raw, ok := kv["config.json"]; ok {
		patch, err := kubeConfigPatch(raw)
		if err != nil {
			log.Error().Err(err).Msg("kubernetes config.json")
		} else if err := pg.validateSettings(ctx, patch); err != nil {
			log.Warn().Err(err).Msg("kubernetes config.json")
		} else {
			kubePropose(ctx, srv, st, keyConfig, patch, "configuration")
		}
	}
	if raw, ok := kv["settings.json"]; ok {
		normalized, err := kubeSettingsPatch(raw)
		if err != nil {
			log.Error().Err(err).Msg("kubernetes settings.json")
		} else {
			kubePropose(ctx, srv, st, agent.KeySettings, normalized, "settings")
		}
	}
}

// kubePropose replaces key with patch, marshalled, when it differs from what
// the store has: the Kubernetes object is the desired state, a key it drops
// leaves the cluster's configuration.
func kubePropose(ctx context.Context, srv *agent.Server, st *store.Store, key string, patch any, what string) {
	data, err := json.Marshal(patch)
	if err != nil {
		log.Error().Err(err).Msg("kubernetes " + what)
		return
	}
	if cur, err := st.Get(key); err == nil && cur == string(data) {
		return
	}
	if err := srv.Apply(ctx, store.Command{Op: "set", Key: key, Value: string(data)}); err != nil {
		log.Error().Err(err).Msg("kubernetes " + what)
		return
	}
	log.Info().Str("key", key).Msg("kubernetes: " + what + " applied")
}

// kubeConfigPatch parses config.json into the shape PATCH /config takes:
// parameter names mapped to values, a null resetting the parameter.
func kubeConfigPatch(raw string) (map[string]*string, error) {
	var patch map[string]*string
	if err := json.Unmarshal([]byte(raw), &patch); err != nil {
		return nil, err
	}
	for name := range patch {
		_, exists := blockedParameters[name]
		if !parameterName.MatchString(name) || exists {
			return nil, fmt.Errorf("parameter %q cannot be set", name)
		}
	}
	return patch, nil
}

// kubeSettingsPatch parses settings.json and normalizes its values the way
// PATCH /settings does: a string goes through the setting's parser, a
// non-string is taken as decoded (a number, a boolean, the hba list), a null
// drops the setting from the desired state.
func kubeSettingsPatch(raw string) (map[string]any, error) {
	var patch map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &patch); err != nil {
		return nil, err
	}
	normalized := make(map[string]any, len(patch))
	for name, value := range patch {
		parse, ok := knownSettings[name]
		if !ok {
			return nil, fmt.Errorf("unknown setting %q", name)
		}
		if string(value) == "null" {
			continue
		}
		var s string
		if err := json.Unmarshal(value, &s); err == nil {
			v, err := parse(s)
			if err != nil {
				return nil, fmt.Errorf("setting %q: %v", name, err)
			}
			normalized[name] = v
			continue
		}
		var v any
		if err := json.Unmarshal(value, &v); err != nil {
			return nil, fmt.Errorf("setting %q: %v", name, err)
		}
		normalized[name] = v
	}
	return normalized, nil
}

// errManagedByKube is how PATCH /config and /settings answer in Kubernetes
// mode: the configmap and the secret are the configuration there.
var errManagedByKube = errors.New("managed by Kubernetes: change the objects of --k8s instead")
