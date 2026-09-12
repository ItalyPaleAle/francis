package workflow

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/builtinactor"
)

// registryState is what the definition registry holds: the fingerprint first recorded for each version of the graph
type registryState struct {
	// Versions maps a version number to the fingerprint that defined it, and is never overwritten, so the first deployment of a version defines it
	Versions map[int]registryEntry `msgpack:"versions,omitempty"`
}

// registryEntry is one version's recorded definition
type registryEntry struct {
	Fingerprint string    `msgpack:"fingerprint"`
	FirstSeenAt time.Time `msgpack:"firstSeenAt"`
}

// registerRequest asks the registry whether this host's graph is the one recorded for a version
type registerRequest struct {
	Version     int    `msgpack:"version"`
	Fingerprint string `msgpack:"fingerprint"`
}

// registerResponse answers the consistency check, carrying what was recorded so a conflict can name both fingerprints
type registerResponse struct {
	OK          bool      `msgpack:"ok"`
	Fingerprint string    `msgpack:"fingerprint,omitempty"`
	FirstSeenAt time.Time `msgpack:"firstSeenAt,omitzero"`
}

// forgetRequest removes a version that was registered wrongly and has no instances left
type forgetRequest struct {
	Version int `msgpack:"version"`
}

// DefinitionInfo describes one version the registry holds, as returned by Definitions
type DefinitionInfo struct {
	// Version is the version number the definition was registered under
	Version int
	// Fingerprint is the hash of the graph first registered for the version
	Fingerprint string
	// FirstSeenAt is when the version was first recorded
	FirstSeenAt time.Time
	// Conflicts reports whether this host's own definition disagrees with what is recorded, which is the operator's view of a version bump that was forgotten
	Conflicts bool
}

// definitionsResponse carries what the registry holds back to the caller
type definitionsResponse struct {
	Entries map[int]registryEntry `msgpack:"entries,omitempty"`
}

// registryActor is the cluster-wide singleton that records each version's definition fingerprint, so two hosts cannot serve different graphs under the same version
type registryActor struct {
	client actor.Client[registryState]
}

// newRegistryActor builds the registry singleton
func newRegistryActor(bareType string, actorID string, svc *actor.Service) actor.Actor {
	return &registryActor{
		client: builtinactor.NewClient[registryState](bareType, actorID, svc),
	}
}

// Invoke answers the registry's three questions: register a version, list what is held, and forget one
func (r *registryActor) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	switch method {
	case methodRegister:
		return r.register(ctx, data)
	case methodDefinitions:
		return r.definitions(ctx)
	case methodForget:
		return nil, r.forget(ctx, data)
	default:
		return nil, fmt.Errorf("unknown workflow registry method %q", method)
	}
}

// register records a version's fingerprint the first time it is seen, and otherwise answers whether the caller's matches
// It never overwrites, so the first deployment of a version defines it and every later disagreement is reported as a conflict
func (r *registryActor) register(ctx context.Context, data actor.Envelope) (any, error) {
	var req registerRequest
	err := decodePayload(data, &req)
	if err != nil {
		return nil, err
	}

	st, err := r.client.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the registry state: %w", err)
	}

	existing, ok := st.Versions[req.Version]
	if ok {
		return registerResponse{
			OK:          existing.Fingerprint == req.Fingerprint,
			Fingerprint: existing.Fingerprint,
			FirstSeenAt: existing.FirstSeenAt,
		}, nil
	}

	if st.Versions == nil {
		st.Versions = map[int]registryEntry{}
	}
	entry := registryEntry{
		Fingerprint: req.Fingerprint,
		FirstSeenAt: time.Now(),
	}
	st.Versions[req.Version] = entry

	err = r.client.SetState(ctx, st, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to record the definition: %w", err)
	}

	return registerResponse{OK: true, Fingerprint: entry.Fingerprint, FirstSeenAt: entry.FirstSeenAt}, nil
}

// definitions returns every version the registry holds, which is the operator's view of what has been deployed
func (r *registryActor) definitions(ctx context.Context) (any, error) {
	st, err := r.client.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the registry state: %w", err)
	}
	return definitionsResponse{Entries: st.Versions}, nil
}

// forget removes a version, which is the operator's reset for one that was registered wrongly and has no instances left
func (r *registryActor) forget(ctx context.Context, data actor.Envelope) error {
	var req forgetRequest
	err := decodePayload(data, &req)
	if err != nil {
		return err
	}

	st, err := r.client.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the registry state: %w", err)
	}

	_, ok := st.Versions[req.Version]
	if !ok {
		return nil
	}

	delete(st.Versions, req.Version)
	err = r.client.SetState(ctx, st, nil)
	if err != nil {
		return fmt.Errorf("failed to forget the definition: %w", err)
	}
	return nil
}

// versionCheck caches this host's answer for one version, for the life of the process
type versionCheck struct {
	once sync.Once
	ok   bool
	err  error
}

// serveVersion reports whether this host may serve a version, asking the registry the first time and caching the answer
//
// This is the one place the engine makes a synchronous call to another actor from a turn (§4.2): a single cached Invoke, bounded to milliseconds, at most once per version for the life of the process
// A host cannot learn the answer from Bootstrap, because the framework drives that hook for singletons and only logs its error, so the check is made where the engine can act on it
func (w *Workflow) serveVersion(ctx context.Context, svc *actor.Service, version int) (bool, error) {
	w.checksMu.Lock()
	check, ok := w.checks[version]
	if !ok {
		check = &versionCheck{}
		if w.checks == nil {
			w.checks = map[int]*versionCheck{}
		}
		w.checks[version] = check
	}
	w.checksMu.Unlock()

	check.once.Do(func() {
		check.ok, check.err = w.askRegistry(ctx, svc, version)
	})

	return check.ok, check.err
}

// askRegistry performs the one consistency check, and turns a conflict into a decline rather than an error so the work re-routes to hosts whose code matches
func (w *Workflow) askRegistry(ctx context.Context, svc *actor.Service, version int) (bool, error) {
	// Only the version this host's code defines can be checked against this host's fingerprint
	// An instance stamped with another version is declined by the caller on the version mismatch itself, without consulting the registry
	if version != w.def.version {
		return false, nil
	}

	res, err := builtinactor.Invoke(ctx, svc, w.registryType(), methodRegister, registerRequest{
		Version:     version,
		Fingerprint: w.def.fingerprint,
	})
	if err != nil {
		return false, fmt.Errorf("failed to check the workflow definition registry: %w", err)
	}

	var resp registerResponse
	err = res.Decode(&resp)
	if err != nil {
		return false, fmt.Errorf("failed to decode the registry response: %w", err)
	}

	if resp.OK {
		return true, nil
	}

	// The failure mode is loud by design: whichever side deployed second conflicts, the metric fires within one turn, nothing corrupts, and the fix is a version bump
	w.metrics.definitionConflicts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", w.name),
		attribute.Int("version", version),
	))
	if w.log != nil {
		w.log.ErrorContext(ctx, "Workflow definition conflicts with the one registered for this version; declining its jobs",
			slog.Int("version", version),
			slog.String("registeredFingerprint", resp.Fingerprint),
			slog.String("localFingerprint", w.def.fingerprint),
			slog.Time("firstSeenAt", resp.FirstSeenAt),
		)
	}
	return false, nil
}
