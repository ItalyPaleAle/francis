package internal

import (
	"bytes"
	"cmp"
	"context"
	"log/slog"
	"slices"
	"strings"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *Provider) GetState(ctx context.Context, r ref.ActorRef) ([]byte, error) {
	key := NewActorKey(r.ActorType, r.ActorID)

	p.StateMu.RLock()
	defer p.StateMu.RUnlock()

	state, ok := p.ActorState[key]
	if !ok {
		return nil, components.ErrNoState
	}

	// Check expiration
	if state.IsExpired(p.Clock.Now()) {
		return nil, components.ErrNoState
	}

	return state.Data, nil
}

func (p *Provider) SetState(ctx context.Context, r ref.ActorRef, data []byte, opts components.SetStateOpts) error {
	key := NewActorKey(r.ActorType, r.ActorID)

	p.stateWriteMu.Lock()
	defer p.stateWriteMu.Unlock()

	// The labels passed in replace whatever the actor had, so a nil one simply leaves none behind
	entry := &StateEntry{
		Data: data,
	}
	if opts.WorkflowLabels != nil && !opts.WorkflowLabels.IsZero() {
		entry.WorkflowLabels = new(*opts.WorkflowLabels)
	}
	if opts.TTL > 0 {
		entry.Expiration = new(p.Clock.Now().Add(opts.TTL))
	}

	changes := NewChanges()
	defer changes.Release()
	changes.ActorState.Set = append(changes.ActorState.Set, ActorStateChange{Key: key, Value: entry})

	// Merge the appended workflow events into the actor's history, recording the ones that are new in the change set
	// Reading the current history under the read lock is enough, since stateWriteMu keeps any other writer out until this one has applied
	var (
		events        []components.WorkflowEvent
		eventsChanged bool
	)
	if len(opts.AppendEvents) > 0 {
		p.StateMu.RLock()
		current := p.WorkflowEvents[key]
		p.StateMu.RUnlock()

		events, eventsChanged = mergeWorkflowEvents(key, current, opts.AppendEvents, changes)
	}

	// Persist first, then apply in memory
	return p.persistThenApply(ctx, &p.StateMu, changes, func() {
		p.ActorState[key] = entry
		if eventsChanged {
			if len(events) > 0 {
				p.WorkflowEvents[key] = events
			} else {
				delete(p.WorkflowEvents, key)
			}
		}
	})
}

// mergeWorkflowEvents computes an actor's event history after appending events to current, recording the reset and the inserts in changes
// When the first appended event has sequence number 1 the previous history is dropped, since it belongs to an earlier state that reused the actor ID
// Events whose sequence number is already stored (or repeated within the batch) are ignored, so a retried write never duplicates them
// It never modifies current within its length, and reports whether the history changed at all
func mergeWorkflowEvents(key ActorKey, current []components.WorkflowEvent, appendEvents []components.WorkflowEvent, changes *Changes) ([]components.WorkflowEvent, bool) {
	// A batch that starts over replaces whatever was stored before
	changed := false
	if appendEvents[0].Seq == 1 {
		changes.WorkflowEvents.Reset = append(changes.WorkflowEvents.Reset, key)
		changed = len(current) > 0
		current = nil
	}

	// Keep only the events that are new, and record them for persistence
	added := newWorkflowEvents(current, appendEvents)
	if len(added) == 0 {
		return current, changed
	}
	changes.WorkflowEvents.Insert = append(changes.WorkflowEvents.Insert, WorkflowEventChange{Key: key, Events: added})

	return mergeSortedEvents(current, added), true
}

// newWorkflowEvents returns the events of batch whose sequence number current does not have, sorted by sequence number and with their data cloned
// A sequence number repeated within the batch keeps its first occurrence
// current must be sorted by sequence number, so each lookup is a binary search rather than a pass over the whole history
func newWorkflowEvents(current []components.WorkflowEvent, batch []components.WorkflowEvent) []components.WorkflowEvent {
	added := make([]components.WorkflowEvent, 0, len(batch))
	for _, ev := range batch {
		_, found := slices.BinarySearchFunc(current, ev.Seq, compareEventSeq)
		if found {
			continue
		}
		ev.Data = bytes.Clone(ev.Data)
		added = append(added, ev)
	}

	// A stable sort keeps repeated sequence numbers in batch order, so compacting keeps the first occurrence
	slices.SortStableFunc(added, func(a, b components.WorkflowEvent) int {
		return cmp.Compare(a.Seq, b.Seq)
	})
	return slices.CompactFunc(added, func(a, b components.WorkflowEvent) bool {
		return a.Seq == b.Seq
	})
}

// mergeSortedEvents returns the history made of current and added, which are both sorted by sequence number and share none
// In the normal case every added event follows the last stored one, and the events are appended, possibly into the spare capacity of current's backing array
// That never changes an element within current's length, which is all a reader holding current can see, so a snapshot taken under the read lock stays valid
// Otherwise the two are merged into a new slice
func mergeSortedEvents(current []components.WorkflowEvent, added []components.WorkflowEvent) []components.WorkflowEvent {
	if len(added) == 0 {
		return current
	}
	if len(current) == 0 || added[0].Seq > current[len(current)-1].Seq {
		return append(current, added...)
	}

	res := make([]components.WorkflowEvent, 0, len(current)+len(added))
	i, j := 0, 0
	for i < len(current) && j < len(added) {
		if current[i].Seq < added[j].Seq {
			res = append(res, current[i])
			i++
		} else {
			res = append(res, added[j])
			j++
		}
	}
	res = append(res, current[i:]...)
	return append(res, added[j:]...)
}

// compareEventSeq compares an event's sequence number with seq, for binary searches over a history sorted by sequence number
func compareEventSeq(ev components.WorkflowEvent, seq int64) int {
	return cmp.Compare(ev.Seq, seq)
}

func (p *Provider) ListStates(ctx context.Context, req components.ListStatesReq) (components.ListStatesRes, error) {
	limit := req.EffectiveLimit()
	now := p.Clock.Now()

	// The created range is compared as strings, in the fixed-width format the label is stored in
	var createdFrom, createdTo string
	if !req.CreatedFrom.IsZero() {
		createdFrom = components.FormatWorkflowCreated(req.CreatedFrom)
	}
	if !req.CreatedTo.IsZero() {
		createdTo = components.FormatWorkflowCreated(req.CreatedTo)
	}

	p.StateMu.RLock()
	defer p.StateMu.RUnlock()

	// Keep the first actor IDs that match the type and cursor, plus one that tells whether more follow, skipping expired state so the listing agrees with GetState even before the background cleanup runs
	// An empty cursor selects the first page, since every actor ID sorts after the empty string
	sel := newPageSelector(limit+1, strings.Compare)
	for key, state := range p.ActorState {
		if key.ActorType != req.ActorType || state.IsExpired(now) {
			continue
		}

		if key.ActorID <= req.After {
			continue
		}

		// A label filter narrows the listing to the actors whose labels match every field it sets, matching what the SQL providers do with an indexed equality
		if !state.MatchesWorkflowLabels(req.WorkflowLabels) || !state.MatchesCreatedRange(createdFrom, createdTo) {
			continue
		}

		sel.Add(key.ActorID)
	}

	// Anything past the limit is dropped from the page, but its existence is reported through HasMore
	matches := sel.Sorted()
	hasMore := len(matches) > limit
	if hasMore {
		matches = matches[:limit]
	}

	res := components.ListStatesRes{
		States:  make([]components.ActorStateInfo, len(matches)),
		HasMore: hasMore,
	}
	for i, actorID := range matches {
		state := p.ActorState[NewActorKey(req.ActorType, actorID)]
		res.States[i] = components.ActorStateInfo{
			ActorID: actorID,
		}

		// The labels are always returned, copied so the caller cannot alter the stored entry
		if state.WorkflowLabels != nil {
			res.States[i].WorkflowLabels = new(*state.WorkflowLabels)
		}

		// The data is cloned because the entry stays live in the map, where a concurrent SetState could otherwise hand the caller a shared slice
		if req.IncludeData {
			res.States[i].Data = bytes.Clone(state.Data)
		}
	}

	return res, nil
}

func (p *Provider) CountStates(_ context.Context, req components.CountStatesReq) (int, error) {
	if req.Limit <= 0 {
		return 0, nil
	}
	now := p.Clock.Now()

	p.StateMu.RLock()
	defer p.StateMu.RUnlock()

	// Count without collecting or sorting the actor IDs, stopping as soon as the limit is reached
	// Expired state is skipped so the count agrees with ListStates even before the background cleanup runs
	var count int
	for key, state := range p.ActorState {
		if key.ActorType != req.ActorType || state.IsExpired(now) || !state.MatchesWorkflowLabels(req.WorkflowLabels) {
			continue
		}

		count++
		if count >= req.Limit {
			return req.Limit, nil
		}
	}

	return count, nil
}

func (p *Provider) DeleteState(ctx context.Context, r ref.ActorRef) error {
	key := NewActorKey(r.ActorType, r.ActorID)

	p.stateWriteMu.Lock()
	defer p.stateWriteMu.Unlock()

	p.StateMu.RLock()
	state, ok := p.ActorState[key]
	expired := ok && state.IsExpired(p.Clock.Now())
	p.StateMu.RUnlock()

	if !ok {
		return components.ErrNoState
	}

	changes := NewChanges()
	defer changes.Release()
	changes.ActorState.Delete = append(changes.ActorState.Delete, key)

	apply := func() {
		p.deleteStateEntry(key)
	}

	// Expired state is treated as absent
	// We still remove it (best-effort), but always return ErrNoState
	if expired {
		err := p.persistThenApply(ctx, &p.StateMu, changes, apply)
		if err != nil {
			// Only log the error here: the expired state isn't returned anyway, and the background cleanup will retry the removal later
			p.Log.WarnContext(ctx, "Error while persisting removal of expired state in DeleteState", slog.Any("error", err))
		}
		return components.ErrNoState
	}

	return p.persistThenApply(ctx, &p.StateMu, changes, apply)
}

func (p *Provider) ListStateActorTypes(ctx context.Context, prefix string) ([]string, error) {
	now := p.Clock.Now()

	p.StateMu.RLock()
	defer p.StateMu.RUnlock()

	// Collect the distinct types with live state, skipping expired rows so the listing agrees with GetState before the background cleanup runs
	seen := make(map[string]struct{})
	for key, state := range p.ActorState {
		if !strings.HasPrefix(key.ActorType, prefix) || state.IsExpired(now) {
			continue
		}
		seen[key.ActorType] = struct{}{}
	}

	// The map has no order of its own, so the ascending order the API promises has to be established here
	res := make([]string, 0, len(seen))
	for actorType := range seen {
		res = append(res, actorType)
	}
	slices.Sort(res)

	return res, nil
}

func (p *Provider) ListWorkflowEvents(ctx context.Context, req components.ListWorkflowEventsReq) (components.ListWorkflowEventsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)
	key := NewActorKey(req.ActorType, req.ActorID)

	p.StateMu.RLock()
	defer p.StateMu.RUnlock()

	// Events are only visible while the state they belong to is live
	state, ok := p.ActorState[key]
	if !ok || state.IsExpired(p.Clock.Now()) {
		return components.ListWorkflowEventsRes{Events: []components.WorkflowEvent{}}, nil
	}

	// The history is sorted by sequence number, so the page starts at the first event after the cursor
	events := p.WorkflowEvents[key]
	start, _ := slices.BinarySearchFunc(events, req.AfterSeq+1, compareEventSeq)
	events = events[start:]

	// Anything past the limit is dropped from the page, but its existence is reported through HasMore
	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}

	// The data is cloned so the caller cannot alter the stored history
	res := components.ListWorkflowEventsRes{
		Events:  make([]components.WorkflowEvent, len(events)),
		HasMore: hasMore,
	}
	for i, ev := range events {
		ev.Data = bytes.Clone(ev.Data)
		res.Events[i] = ev
	}

	return res, nil
}
