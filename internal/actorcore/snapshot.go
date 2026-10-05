package actorcore

import (
	"cmp"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/italypaleale/francis/protocol"
)

// ManagementDefinitionProvider is implemented by built-in actors that serve a versioned definition the management API reports, such as a workflow
// The host detects it when the built-in actor is registered, so host packages never import the built-in actor's package
type ManagementDefinitionProvider interface {
	ManagementDefinition() (name string, version int, fingerprint string)
}

// RecordManagementDefinition records the definition served by a registered built-in actor, if it implements ManagementDefinitionProvider
// Like the other registration methods it must be called before Start
func (m *Manager) RecordManagementDefinition(b any) {
	p, ok := b.(ManagementDefinitionProvider)
	if !ok {
		return
	}

	name, version, fingerprint := p.ManagementDefinition()
	m.workflowDefinitions = append(m.workflowDefinitions, protocol.WorkflowDefinitionInfo{
		Name:        name,
		Version:     version,
		Fingerprint: fingerprint,
	})
}

// WorkflowDefinitions returns the workflow definitions served by this host, ordered by name and then version
// The returned slice is a copy the caller may modify
func (m *Manager) WorkflowDefinitions() []protocol.WorkflowDefinitionInfo {
	if len(m.workflowDefinitions) == 0 {
		return nil
	}

	res := slices.Clone(m.workflowDefinitions)
	slices.SortFunc(res, func(a, b protocol.WorkflowDefinitionInfo) int {
		return cmp.Or(
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.Version, b.Version),
		)
	})
	return res
}

// CapacityGroups returns the host-local capacity groups with their current usage, ordered by group name
// Usage is read from the length of each group's semaphore channel, so it is a point-in-time value that can change immediately
func (m *Manager) CapacityGroups() []protocol.CapacityGroupInfo {
	if len(m.capacityGroups) == 0 {
		return nil
	}

	// Collect the actor types enrolled in each group
	types := make(map[string][]string, len(m.capacityGroups))
	for actorType, group := range m.actorTypeCapacityGroup {
		types[group] = append(types[group], actorType)
	}

	// Build one entry per group, reading the semaphore's capacity and current occupancy
	res := make([]protocol.CapacityGroupInfo, 0, len(m.capacityGroups))
	for name, sem := range m.capacityGroups {
		groupTypes := types[name]
		slices.Sort(groupTypes)
		res = append(res, protocol.CapacityGroupInfo{
			Name:       name,
			Limit:      cap(sem.slots),
			InUse:      len(sem.slots),
			ActorTypes: groupTypes,
		})
	}
	slices.SortFunc(res, func(a, b protocol.CapacityGroupInfo) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return res
}

// Snapshot returns a page of the actors held in memory by this host, together with its capacity groups
// It reads the active actors map without taking any turn lock and without activating anything, so it never perturbs the actors it reports
// Activations are ordered by actor type and then actor ID, and the cursor is the "type/id" key of the last activation returned
// HostSnapshot fills in what the host reports about itself
func (m *Manager) Snapshot(req protocol.HostSnapshotRequest) protocol.HostSnapshotResponse {
	// Clamp the page size to the protocol maximum
	limit := req.Limit
	if limit <= 0 || limit > protocol.MaxSnapshotPageSize {
		limit = protocol.MaxSnapshotPageSize
	}

	// Actor types and IDs cannot contain "/", so the cursor splits unambiguously at the first one
	var (
		afterType string
		afterID   string
	)
	hasCursor := req.After != ""
	if hasCursor {
		afterType, afterID, _ = strings.Cut(req.After, "/")
	}

	// Count every matching activation, and keep only the ones after the cursor as candidates for this page
	var count int
	candidates := make([]*ActiveActor, 0)
	for _, act := range m.Actors.Iterator() {
		if act == nil {
			continue
		}
		if req.ActorType != "" && act.ActorType() != req.ActorType {
			continue
		}
		count++

		if req.SkipActivations {
			continue
		}
		if hasCursor && compareActorRef(act.ref.ActorType, act.ref.ActorID, afterType, afterID) <= 0 {
			continue
		}

		candidates = append(candidates, act)
	}

	res := protocol.HostSnapshotResponse{
		ActiveCount:    count,
		CapacityGroups: m.CapacityGroups(),
	}
	if req.SkipActivations || len(candidates) == 0 {
		return res
	}

	// Order by type and then ID, which differs from ordering by the joined key when a type is a prefix of another
	slices.SortFunc(candidates, func(a, b *ActiveActor) int {
		return compareActorRef(a.ref.ActorType, a.ref.ActorID, b.ref.ActorType, b.ref.ActorID)
	})

	// Cut the page, returning a cursor only when more activations remain
	if len(candidates) > limit {
		candidates = candidates[:limit]
		res.Next = candidates[limit-1].Key()
	}

	res.Activations = make([]protocol.ActivationInfo, len(candidates))
	for i, act := range candidates {
		res.Activations[i] = protocol.ActivationInfo{
			ActorType:         act.ref.ActorType,
			ActorID:           act.ref.ActorID,
			ActivatedAtUnixMs: act.ActivatedAt().UnixMilli(),
			Deactivating:      act.Deactivating(),
		}
	}

	return res
}

// HostSnapshot returns a page of this host's activations like Snapshot, completed with what the host reports about itself
func (m *Manager) HostSnapshot(req protocol.HostSnapshotRequest, hostID string, draining bool) protocol.HostSnapshotResponse {
	res := m.Snapshot(req)
	res.HostID = hostID
	res.ObservedAtUnixMs = m.clock.Now().UnixMilli()
	res.Draining = draining
	res.Workflows = m.WorkflowDefinitions()
	return res
}

// compareActorRef orders actor references by type and then by ID
func compareActorRef(aType string, aID string, bType string, bID string) int {
	return cmp.Or(
		strings.Compare(aType, bType),
		strings.Compare(aID, bID),
	)
}

// HaltAllWithin halts all actors active on the host like HaltAll, but stops waiting once timeout elapses
// When the timeout expires, it cancels the in-flight calls of the actors still halting without waiting out the shutdown grace period, and returns their keys so the caller can log them
// Those actors finish halting in the background
// A timeout that is not positive waits for HaltAll to complete
func (m *Manager) HaltAllWithin(timeout time.Duration) (forced []string, err error) {
	if timeout <= 0 {
		return nil, m.HaltAll()
	}

	// Halt in the background so the wait can be bounded
	done := make(chan error, 1)
	go func() {
		done <- m.HaltAll()
	}()

	t := m.clock.NewTimer(timeout)
	select {
	case err = <-done:
		t.Stop()
		return nil, err
	case <-t.C():
	}

	// Every actor still in the table has not finished halting, so report it as forcibly halted
	for key, act := range m.Actors.Iterator() {
		if act == nil {
			continue
		}
		forced = append(forced, key)
	}
	slices.Sort(forced)

	// Cut the remaining in-flight calls short instead of letting them run out the grace period
	m.forceHaltOnce.Do(func() {
		if m.forceHalt != nil {
			close(m.forceHalt)
		}
	})

	return forced, nil
}

// DrainAll halts every actor active on the host like HaltAllWithin, logging a failure and the actors whose in-flight calls were cut short
func (m *Manager) DrainAll(timeout time.Duration) {
	forced, err := m.HaltAllWithin(timeout)
	if err != nil {
		m.log.Warn("Error halting actors", slog.Any("error", err))
	}
	if len(forced) > 0 {
		m.log.Warn(
			"Drain timeout expired: forcibly halting actors that were still busy",
			slog.Duration("timeout", timeout),
			slog.Int("count", len(forced)),
			slog.Any("actors", forced),
		)
	}
}
