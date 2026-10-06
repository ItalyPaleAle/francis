package comptesting

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

// mgmtTimeTolerance absorbs the millisecond truncation some providers apply to the timestamps they store
const mgmtTimeTolerance = time.Millisecond

// assertTimeNear asserts that two instants are the same, up to the precision providers store timestamps with
func assertTimeNear(t *testing.T, want time.Time, got time.Time, msgAndArgs ...any) {
	t.Helper()
	assert.WithinDuration(t, want, got, mgmtTimeTolerance, msgAndArgs...)
}

// assertTimePtrNear is assertTimeNear for optional timestamps, where nil must match nil
func assertTimePtrNear(t *testing.T, want *time.Time, got *time.Time, msg string) {
	t.Helper()
	if want == nil {
		assert.Nilf(t, got, "%s: expected nil", msg)
		return
	}
	if assert.NotNilf(t, got, "%s: expected non-nil", msg) {
		assertTimeNear(t, *want, *got, msg)
	}
}

// TestGetExclusiveLease covers reading the holder of the cluster exclusive-access lease
func (s Suite) TestGetExclusiveLease(t *testing.T) {
	ctx := t.Context()
	err := s.p.Seed(ctx, Spec{})
	require.NoError(t, err)

	// The lease lives outside the seeded tables, so let any lease an earlier test left behind expire first
	leftover, err := s.p.GetExclusiveLease(ctx)
	require.NoError(t, err)
	if leftover.IsHeld() {
		err = s.p.AdvanceClock(leftover.ExpiresAt.Sub(s.p.Now()) + time.Second)
		require.NoError(t, err)
	}

	t.Run("returns a zero value when no lease is held", func(t *testing.T) {
		info, err := s.p.GetExclusiveLease(t.Context())
		require.NoError(t, err)
		assert.Equal(t, components.ExclusiveLeaseInfo{}, info)
		assert.False(t, info.IsHeld())
	})

	t.Run("returns the holder and expiry of a live lease", func(t *testing.T) {
		ctx := t.Context()

		expiresAt, err := s.p.AcquireExclusiveLease(ctx, "mgmt-owner", 2*time.Minute)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.p.ReleaseExclusiveLease(context.WithoutCancel(t.Context()), "mgmt-owner") })

		info, err := s.p.GetExclusiveLease(ctx)
		require.NoError(t, err)
		assert.True(t, info.IsHeld())
		assert.Equal(t, "mgmt-owner", info.Owner)
		assertTimeNear(t, expiresAt, info.ExpiresAt)

		// A renewal moves the reported expiry forward
		err = s.p.AdvanceClock(30 * time.Second)
		require.NoError(t, err)
		renewedAt, err := s.p.RenewExclusiveLease(ctx, "mgmt-owner", 2*time.Minute)
		require.NoError(t, err)
		require.True(t, renewedAt.After(expiresAt))

		info, err = s.p.GetExclusiveLease(ctx)
		require.NoError(t, err)
		assert.Equal(t, "mgmt-owner", info.Owner)
		assertTimeNear(t, renewedAt, info.ExpiresAt)
	})

	t.Run("returns a zero value once the lease is released", func(t *testing.T) {
		ctx := t.Context()

		_, err := s.p.AcquireExclusiveLease(ctx, "mgmt-owner", 2*time.Minute)
		require.NoError(t, err)
		err = s.p.ReleaseExclusiveLease(ctx, "mgmt-owner")
		require.NoError(t, err)

		info, err := s.p.GetExclusiveLease(ctx)
		require.NoError(t, err)
		assert.Equal(t, components.ExclusiveLeaseInfo{}, info)
	})

	t.Run("returns a zero value once the lease expired", func(t *testing.T) {
		ctx := t.Context()

		_, err := s.p.AcquireExclusiveLease(ctx, "mgmt-owner", time.Minute)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.p.ReleaseExclusiveLease(context.WithoutCancel(t.Context()), "mgmt-owner") })

		// The row still names the owner, so this asserts the read compares the expiry with the provider clock
		err = s.p.AdvanceClock(61 * time.Second)
		require.NoError(t, err)

		info, err := s.p.GetExclusiveLease(ctx)
		require.NoError(t, err)
		assert.Equal(t, components.ExclusiveLeaseInfo{}, info)
	})

	t.Run("another owner cannot change the reported holder", func(t *testing.T) {
		ctx := t.Context()

		expiresAt, err := s.p.AcquireExclusiveLease(ctx, "mgmt-owner", 2*time.Minute)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.p.ReleaseExclusiveLease(context.WithoutCancel(t.Context()), "mgmt-owner") })

		// A rejected acquire and a release by an owner that does not hold the lease both leave it as it was
		_, err = s.p.AcquireExclusiveLease(ctx, "mgmt-intruder", 2*time.Minute)
		require.ErrorIs(t, err, components.ErrExclusiveHeld)
		err = s.p.ReleaseExclusiveLease(ctx, "mgmt-intruder")
		require.NoError(t, err)

		info, err := s.p.GetExclusiveLease(ctx)
		require.NoError(t, err)
		assert.Equal(t, "mgmt-owner", info.Owner)
		assertTimeNear(t, expiresAt, info.ExpiresAt)
	})

	t.Run("returns the new holder once an expired lease is taken over", func(t *testing.T) {
		ctx := t.Context()

		_, err := s.p.AcquireExclusiveLease(ctx, "mgmt-owner", time.Minute)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.p.ReleaseExclusiveLease(context.WithoutCancel(t.Context()), "mgmt-owner") })

		err = s.p.AdvanceClock(61 * time.Second)
		require.NoError(t, err)

		expiresAt, err := s.p.AcquireExclusiveLease(ctx, "mgmt-successor", time.Minute)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.p.ReleaseExclusiveLease(context.WithoutCancel(t.Context()), "mgmt-successor") })

		info, err := s.p.GetExclusiveLease(ctx)
		require.NoError(t, err)
		assert.Equal(t, "mgmt-successor", info.Owner)
		assertTimeNear(t, expiresAt, info.ExpiresAt)

		// The previous holder releasing its lost lease does not clear the new one
		err = s.p.ReleaseExclusiveLease(ctx, "mgmt-owner")
		require.NoError(t, err)
		info, err = s.p.GetExclusiveLease(ctx)
		require.NoError(t, err)
		assert.Equal(t, "mgmt-successor", info.Owner)
	})
}

// TestHostDetails covers ListHostDetails and GetHostDetails
func (s Suite) TestHostDetails(t *testing.T) {
	// hostIDsOf returns the host IDs of a page, in the order they were returned
	hostIDsOf := func(hosts []components.HostDetails) []string {
		ids := make([]string, len(hosts))
		for i, h := range hosts {
			ids[i] = h.HostID
		}
		return ids
	}

	// getDetails reads one host through both methods and asserts they agree
	getDetails := func(t *testing.T, ctx context.Context, hostID string) components.HostDetails {
		t.Helper()

		got, err := s.p.GetHostDetails(ctx, hostID)
		require.NoError(t, err)

		res, err := s.p.ListHostDetails(ctx, components.ListHostDetailsReq{})
		require.NoError(t, err)
		idx := slices.IndexFunc(res.Hosts, func(h components.HostDetails) bool { return h.HostID == hostID })
		require.GreaterOrEqualf(t, idx, 0, "host %s is missing from ListHostDetails", hostID)

		// Both methods describe the host the same way
		listed := res.Hosts[idx]
		assertTimeNear(t, got.LastHealthCheck, listed.LastHealthCheck)
		listed.LastHealthCheck = got.LastHealthCheck
		assert.Equal(t, got, listed)

		return got
	}

	t.Run("returns the registration details of a live host", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, Spec{})
		require.NoError(t, err)

		// Register the types out of order, so the response has to sort them, and with positive, negative and zero retentions, since each is meaningful
		res, err := s.p.RegisterHost(ctx, components.RegisterHostReq{
			Address:   "10.1.0.1:5000",
			SessionID: "session-1",
			RuntimeID: "runtime-1",
			ActorTypes: []components.ActorHostType{
				{ActorType: "HD-B", IdleTimeout: 2 * time.Minute, ConcurrencyLimit: 4, CompletedJobRetention: time.Hour, DeadLetteredJobRetention: 48 * time.Hour},
				{ActorType: "HD-A", IdleTimeout: 5 * time.Minute, CompletedJobRetention: -time.Second, DeadLetteredJobRetention: -time.Second},
				{ActorType: "HD-C", IdleTimeout: time.Minute, ConcurrencyLimit: 1},
			},
		})
		require.NoError(t, err)

		got := getDetails(t, ctx, res.HostID)
		assert.Equal(t, res.HostID, got.HostID)
		assert.Equal(t, "10.1.0.1:5000", got.Address)
		assert.Equal(t, "session-1", got.SessionID)
		assert.Equal(t, "runtime-1", got.RuntimeID)
		assert.False(t, got.Draining)
		assertTimeNear(t, s.p.Now(), got.LastHealthCheck)
		assert.Equal(t, []components.HostActorTypeDetails{
			{ActorType: "HD-A", IdleTimeout: 5 * time.Minute, CompletedJobRetention: -time.Second, DeadLetteredJobRetention: -time.Second},
			{ActorType: "HD-B", IdleTimeout: 2 * time.Minute, ConcurrencyLimit: 4, CompletedJobRetention: time.Hour, DeadLetteredJobRetention: 48 * time.Hour},
			{ActorType: "HD-C", IdleTimeout: time.Minute, ConcurrencyLimit: 1},
		}, got.ActorTypes)

		// A health check moves the reported time forward
		err = s.p.AdvanceClock(10 * time.Second)
		require.NoError(t, err)
		err = s.p.UpdateActorHost(ctx, res.HostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
		require.NoError(t, err)
		got = getDetails(t, ctx, res.HostID)
		assertTimeNear(t, s.p.Now(), got.LastHealthCheck)
	})

	t.Run("sub-resolution retentions preserve their policies across every registration write", func(t *testing.T) {
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		types := []components.ActorHostType{{ActorType: "HD-Sentinel", CompletedJobRetention: time.Duration(-1), DeadLetteredJobRetention: time.Nanosecond}}
		res, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{Address: "10.1.9.1:5000", ActorTypes: types})
		require.NoError(t, err)
		assertPolicies := func() {
			t.Helper()
			h := getDetails(t, t.Context(), res.HostID)
			require.Len(t, h.ActorTypes, 1)
			assert.Negative(t, h.ActorTypes[0].CompletedJobRetention)
			assert.Positive(t, h.ActorTypes[0].DeadLetteredJobRetention)
		}
		assertPolicies()
		err = s.p.UpdateActorHost(t.Context(), res.HostID, components.UpdateActorHostReq{ActorTypes: types})
		require.NoError(t, err)
		assertPolicies()
		_, err = s.p.RegisterHost(t.Context(), components.RegisterHostReq{ExistingHostID: res.HostID, Address: "10.1.9.1:5000", ActorTypes: types})
		require.NoError(t, err)
		assertPolicies()
	})

	t.Run("a host without a runtime reports empty session and runtime IDs", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, Spec{})
		require.NoError(t, err)

		res, err := s.p.RegisterHost(ctx, components.RegisterHostReq{
			Address:    "10.1.0.2:5000",
			ActorTypes: []components.ActorHostType{{ActorType: "HD-A", IdleTimeout: time.Minute}},
		})
		require.NoError(t, err)

		got := getDetails(t, ctx, res.HostID)
		assert.Empty(t, got.SessionID)
		assert.Empty(t, got.RuntimeID)
	})

	t.Run("a host with no actor types reports none", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, Spec{})
		require.NoError(t, err)

		res, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "10.1.0.3:5000"})
		require.NoError(t, err)

		got := getDetails(t, ctx, res.HostID)
		assert.Empty(t, got.ActorTypes)
	})

	t.Run("the session and runtime IDs are replaced on reattach", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, Spec{})
		require.NoError(t, err)

		types := []components.ActorHostType{{ActorType: "HD-A", IdleTimeout: time.Minute}}
		res, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "10.1.0.4:5000", SessionID: "s1", RuntimeID: "rt-1", ActorTypes: types})
		require.NoError(t, err)

		// Reattaching through another runtime moves the ownership to it
		re, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "10.1.0.4:5000", ExistingHostID: res.HostID, SessionID: "s2", RuntimeID: "rt-2", ActorTypes: types})
		require.NoError(t, err)
		require.True(t, re.Reattached)
		require.Equal(t, res.HostID, re.HostID)

		got := getDetails(t, ctx, res.HostID)
		assert.Equal(t, "s2", got.SessionID)
		assert.Equal(t, "rt-2", got.RuntimeID)

		// A reattach without a runtime clears the previous owner rather than keeping it
		re, err = s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "10.1.0.4:5000", ExistingHostID: res.HostID, SessionID: "s3", ActorTypes: types})
		require.NoError(t, err)
		require.True(t, re.Reattached)

		got = getDetails(t, ctx, res.HostID)
		assert.Equal(t, "s3", got.SessionID)
		assert.Empty(t, got.RuntimeID)
	})

	t.Run("job retentions follow every write of the actor types", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, Spec{})
		require.NoError(t, err)

		res, err := s.p.RegisterHost(ctx, components.RegisterHostReq{
			Address:    "10.1.0.5:5000",
			ActorTypes: []components.ActorHostType{{ActorType: "HD-A", IdleTimeout: time.Minute, CompletedJobRetention: time.Hour, DeadLetteredJobRetention: 2 * time.Hour}},
		})
		require.NoError(t, err)

		// Replacing the actor types rewrites the retentions
		err = s.p.UpdateActorHost(ctx, res.HostID, components.UpdateActorHostReq{
			ActorTypes: []components.ActorHostType{
				{ActorType: "HD-A", IdleTimeout: time.Minute, CompletedJobRetention: -time.Second, DeadLetteredJobRetention: 3 * time.Hour},
				{ActorType: "HD-B", IdleTimeout: time.Minute},
			},
		})
		require.NoError(t, err)

		got := getDetails(t, ctx, res.HostID)
		assert.Equal(t, []components.HostActorTypeDetails{
			{ActorType: "HD-A", IdleTimeout: time.Minute, CompletedJobRetention: -time.Second, DeadLetteredJobRetention: 3 * time.Hour},
			{ActorType: "HD-B", IdleTimeout: time.Minute},
		}, got.ActorTypes)

		// A health check alone leaves them untouched
		err = s.p.UpdateActorHost(ctx, res.HostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
		require.NoError(t, err)
		got = getDetails(t, ctx, res.HostID)
		require.Len(t, got.ActorTypes, 2)
		assert.Equal(t, -time.Second, got.ActorTypes[0].CompletedJobRetention)
		assert.Equal(t, 3*time.Hour, got.ActorTypes[0].DeadLetteredJobRetention)

		// A reattach registers them again
		re, err := s.p.RegisterHost(ctx, components.RegisterHostReq{
			Address:        "10.1.0.5:5000",
			ExistingHostID: res.HostID,
			ActorTypes:     []components.ActorHostType{{ActorType: "HD-A", IdleTimeout: time.Minute, CompletedJobRetention: 5 * time.Minute, DeadLetteredJobRetention: -time.Second}},
		})
		require.NoError(t, err)
		require.True(t, re.Reattached)

		got = getDetails(t, ctx, res.HostID)
		assert.Equal(t, []components.HostActorTypeDetails{
			{ActorType: "HD-A", IdleTimeout: time.Minute, CompletedJobRetention: 5 * time.Minute, DeadLetteredJobRetention: -time.Second},
		}, got.ActorTypes)
	})

	t.Run("draining hosts are listed with their flag", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, Spec{})
		require.NoError(t, err)

		types := []components.ActorHostType{{ActorType: "HD-A", IdleTimeout: time.Minute}}
		draining, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "10.1.0.6:5000", ActorTypes: types})
		require.NoError(t, err)
		other, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "10.1.0.7:5000", ActorTypes: types})
		require.NoError(t, err)

		err = s.p.UpdateActorHost(ctx, draining.HostID, components.UpdateActorHostReq{Draining: true})
		require.NoError(t, err)

		// ListHosts hides nothing here either, but only the details say which host is draining
		res, err := s.p.ListHostDetails(ctx, components.ListHostDetailsReq{})
		require.NoError(t, err)
		require.Len(t, res.Hosts, 2)
		for _, h := range res.Hosts {
			assert.Equalf(t, h.HostID == draining.HostID, h.Draining, "draining flag of host %s", h.HostID)
		}

		got := getDetails(t, ctx, draining.HostID)
		assert.True(t, got.Draining)
		got = getDetails(t, ctx, other.HostID)
		assert.False(t, got.Draining)
	})

	t.Run("expired hosts are hidden before they are garbage collected", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, Spec{})
		require.NoError(t, err)

		types := []components.ActorHostType{{ActorType: "HD-A", IdleTimeout: time.Minute}}
		stale, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "10.1.0.8:5000", ActorTypes: types})
		require.NoError(t, err)
		live, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "10.1.0.9:5000", ActorTypes: types})
		require.NoError(t, err)

		// Draining does not keep a host visible once its health check is past the deadline
		err = s.p.UpdateActorHost(ctx, stale.HostID, components.UpdateActorHostReq{Draining: true})
		require.NoError(t, err)

		// Only the live host checks in before its deadline, so the stale one ends up past it while the live one does not
		err = s.p.AdvanceClock(40 * time.Second)
		require.NoError(t, err)
		err = s.p.UpdateActorHost(ctx, live.HostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
		require.NoError(t, err)
		err = s.p.AdvanceClock(40 * time.Second)
		require.NoError(t, err)

		res, err := s.p.ListHostDetails(ctx, components.ListHostDetailsReq{})
		require.NoError(t, err)
		assert.Equal(t, []string{live.HostID}, hostIDsOf(res.Hosts))
		assert.False(t, res.HasMore)

		_, err = s.p.GetHostDetails(ctx, stale.HostID)
		require.ErrorIs(t, err, components.ErrHostUnregistered)
	})

	t.Run("an unknown host returns ErrHostUnregistered", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, Spec{})
		require.NoError(t, err)

		_, err = s.p.GetHostDetails(ctx, SpecHostNonExistent)
		require.ErrorIs(t, err, components.ErrHostUnregistered)

		_, err = s.p.GetHostDetails(ctx, "")
		require.ErrorIs(t, err, components.ErrHostUnregistered)

		res, err := s.p.ListHostDetails(ctx, components.ListHostDetailsReq{})
		require.NoError(t, err)
		assert.Empty(t, res.Hosts)
		assert.False(t, res.HasMore)
	})

	t.Run("an unregistered host returns ErrHostUnregistered", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, Spec{})
		require.NoError(t, err)

		types := []components.ActorHostType{{ActorType: "HD-A", IdleTimeout: time.Minute}}
		gone, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "10.1.0.10:5000", ActorTypes: types})
		require.NoError(t, err)
		kept, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "10.1.0.11:5000", ActorTypes: types})
		require.NoError(t, err)

		err = s.p.UnregisterHost(ctx, gone.HostID, components.UnregisterHostOpts{})
		require.NoError(t, err)

		_, err = s.p.GetHostDetails(ctx, gone.HostID)
		require.ErrorIs(t, err, components.ErrHostUnregistered)

		res, err := s.p.ListHostDetails(ctx, components.ListHostDetailsReq{})
		require.NoError(t, err)
		assert.Equal(t, []string{kept.HostID}, hostIDsOf(res.Hosts))
	})

	t.Run("counts the actors placed on each type", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, Spec{
			Hosts: HostSpecCollection{
				{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: time.Second},
				{HostID: SpecHostH2, Address: "127.0.0.1:4002", LastHealthAgo: time.Second},
			},
			HostActorTypes: HostActorTypeSpecCollection{
				{HostID: SpecHostH1, ActorType: "HD-A", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 10},
				{HostID: SpecHostH1, ActorType: "HD-B", ActorIdleTimeout: 3 * time.Minute},
				{HostID: SpecHostH1, ActorType: "HD-C", ActorIdleTimeout: time.Minute},
				{HostID: SpecHostH2, ActorType: "HD-A", ActorIdleTimeout: 5 * time.Minute},
			},
			ActiveActors: []ActiveActorSpec{
				{ActorType: "HD-A", ActorID: "a1", HostID: SpecHostH1, ActorIdleTimeout: 5 * time.Minute},
				{ActorType: "HD-A", ActorID: "a2", HostID: SpecHostH1, ActorIdleTimeout: 5 * time.Minute},
				{ActorType: "HD-B", ActorID: "b1", HostID: SpecHostH1, ActorIdleTimeout: 3 * time.Minute},
				{ActorType: "HD-A", ActorID: "a3", HostID: SpecHostH2, ActorIdleTimeout: 5 * time.Minute},
			},
		})
		require.NoError(t, err)

		// activeCounts maps each actor type of a host to its active count
		activeCounts := func(t *testing.T, hostID string) map[string]int {
			t.Helper()
			got := getDetails(t, ctx, hostID)
			res := make(map[string]int, len(got.ActorTypes))
			for _, at := range got.ActorTypes {
				res[at.ActorType] = at.ActiveCount
			}
			return res
		}

		// Actors on another host are not counted, and a type with no actor reports zero
		assert.Equal(t, map[string]int{"HD-A": 2, "HD-B": 1, "HD-C": 0}, activeCounts(t, SpecHostH1))
		assert.Equal(t, map[string]int{"HD-A": 1}, activeCounts(t, SpecHostH2))

		// A new placement and a removal are both reflected
		lookup, err := s.p.LookupActor(ctx, ref.NewActorRef("HD-C", "c1"), components.LookupActorOpts{Hosts: []string{SpecHostH1}})
		require.NoError(t, err)
		require.Equal(t, SpecHostH1, lookup.HostID)
		err = s.p.RemoveActor(ctx, ref.NewActorRef("HD-A", "a1"))
		require.NoError(t, err)

		assert.Equal(t, map[string]int{"HD-A": 1, "HD-B": 1, "HD-C": 1}, activeCounts(t, SpecHostH1))
		assert.Equal(t, map[string]int{"HD-A": 1}, activeCounts(t, SpecHostH2))
	})

	t.Run("pages through hosts in host ID order", func(t *testing.T) {
		ctx := t.Context()

		// Seed the hosts out of order, with an expired one in the middle of the range
		err := s.p.Seed(ctx, Spec{
			Hosts: HostSpecCollection{
				{HostID: SpecHostH7, Address: "127.0.0.1:4007", LastHealthAgo: time.Second},
				{HostID: SpecHostH3, Address: "127.0.0.1:4003", LastHealthAgo: time.Second},
				{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: time.Second},
				{HostID: SpecHostH5, Address: "127.0.0.1:4005", LastHealthAgo: 24 * time.Hour},
				{HostID: SpecHostH4, Address: "127.0.0.1:4004", LastHealthAgo: time.Second},
				{HostID: SpecHostH2, Address: "127.0.0.1:4002", LastHealthAgo: time.Second},
			},
		})
		require.NoError(t, err)
		want := []string{SpecHostH1, SpecHostH2, SpecHostH3, SpecHostH4, SpecHostH7}

		// A full listing returns every live host in order
		res, err := s.p.ListHostDetails(ctx, components.ListHostDetailsReq{})
		require.NoError(t, err)
		assert.Equal(t, want, hostIDsOf(res.Hosts))
		assert.False(t, res.HasMore)

		// Walk the collection two at a time, using the last ID of each page as the cursor for the next one
		var (
			seen    []string
			hasMore []bool
			cursor  string
		)
		for range want {
			res, err = s.p.ListHostDetails(ctx, components.ListHostDetailsReq{After: cursor, Limit: 2})
			require.NoError(t, err)
			require.NotEmpty(t, res.Hosts)
			require.LessOrEqual(t, len(res.Hosts), 2)

			seen = append(seen, hostIDsOf(res.Hosts)...)
			hasMore = append(hasMore, res.HasMore)
			cursor = res.Hosts[len(res.Hosts)-1].HostID
			if !res.HasMore {
				break
			}
		}
		assert.Equal(t, want, seen)
		assert.Equal(t, []bool{true, true, false}, hasMore)

		// A page that ends exactly at the last host reports no more results
		res, err = s.p.ListHostDetails(ctx, components.ListHostDetailsReq{After: SpecHostH2, Limit: 3})
		require.NoError(t, err)
		assert.Equal(t, []string{SpecHostH3, SpecHostH4, SpecHostH7}, hostIDsOf(res.Hosts))
		assert.False(t, res.HasMore)

		// The cursor does not need to be a registered host
		res, err = s.p.ListHostDetails(ctx, components.ListHostDetailsReq{After: SpecHostNonExistent})
		require.NoError(t, err)
		assert.Equal(t, want, hostIDsOf(res.Hosts))

		// A cursor at the end of the collection returns nothing
		res, err = s.p.ListHostDetails(ctx, components.ListHostDetailsReq{After: SpecHostH7})
		require.NoError(t, err)
		assert.Empty(t, res.Hosts)
		assert.False(t, res.HasMore)

		// A negative limit is served with the default page size rather than failing
		res, err = s.p.ListHostDetails(ctx, components.ListHostDetailsReq{Limit: -1})
		require.NoError(t, err)
		assert.Equal(t, want, hostIDsOf(res.Hosts))
		assert.False(t, res.HasMore)
	})
}

// TestMarkHostDraining covers MarkHostDraining and ClearHostDraining
func (s Suite) TestMarkHostDraining(t *testing.T) {
	// register registers a live host serving the given actor types
	register := func(t *testing.T, address string, actorTypes ...string) string {
		t.Helper()
		ats := make([]components.ActorHostType, len(actorTypes))
		for i, at := range actorTypes {
			ats[i] = components.ActorHostType{ActorType: at, IdleTimeout: time.Minute}
		}
		res, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{Address: address, ActorTypes: ats})
		require.NoError(t, err)
		return res.HostID
	}

	// draining reads a host's draining flag
	draining := func(t *testing.T, hostID string) bool {
		t.Helper()
		h, err := s.p.GetHostDetails(t.Context(), hostID)
		require.NoError(t, err)
		return h.Draining
	}

	t.Run("refuses to drain the last server of a type unless forced", func(t *testing.T) {
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		h1 := register(t, "10.3.0.1:5000", "MD-A", "MD-B", "MD-C")
		register(t, "10.3.0.2:5000", "MD-B")

		// A draining host does not count as a server of the types it registered
		h3 := register(t, "10.3.0.3:5000", "MD-C")
		err = s.p.UpdateActorHost(t.Context(), h3, components.UpdateActorHostReq{Draining: true})
		require.NoError(t, err)

		// Without force the host is left alone, and the types without another server are reported in order
		req := components.MarkHostDrainingReq{HostID: h1}
		res, err := s.p.MarkHostDraining(t.Context(), req)
		require.NoError(t, err)
		assert.False(t, res.AlreadyDraining)
		assert.Equal(t, []string{"MD-A", "MD-C"}, res.LastServerOf)
		assert.True(t, res.Refused(req))
		assert.False(t, draining(t, h1))

		// With force the host is marked draining, and the same types are reported
		req.Force = true
		res, err = s.p.MarkHostDraining(t.Context(), req)
		require.NoError(t, err)
		assert.Equal(t, []string{"MD-A", "MD-C"}, res.LastServerOf)
		assert.False(t, res.Refused(req))
		assert.True(t, draining(t, h1))

		// A host that is already draining is reported as such, without a check
		res, err = s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: h1})
		require.NoError(t, err)
		assert.True(t, res.AlreadyDraining)
		assert.Empty(t, res.LastServerOf)
	})

	t.Run("marks a host whose types are served elsewhere", func(t *testing.T) {
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		h1 := register(t, "10.3.1.1:5000", "MD-A")
		register(t, "10.3.1.2:5000", "MD-A")

		res, err := s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: h1})
		require.NoError(t, err)
		assert.Empty(t, res.LastServerOf)
		assert.True(t, draining(t, h1))
	})

	t.Run("clearing the flag puts the host back into service", func(t *testing.T) {
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		h1 := register(t, "10.3.4.1:5000", "MD-A")
		h2 := register(t, "10.3.4.2:5000", "MD-A")

		// Marking h1 leaves h2 as the only server, so h2 can't be drained without force
		mark, err := s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: h1})
		require.NoError(t, err)
		require.True(t, draining(t, h1))
		req := components.MarkHostDrainingReq{HostID: h2}
		res, err := s.p.MarkHostDraining(t.Context(), req)
		require.NoError(t, err)
		require.True(t, res.Refused(req))

		// Clearing h1 counts it as a server again
		cleared, err := s.p.ClearHostDraining(t.Context(), h1, mark.RollbackToken)
		require.NoError(t, err)
		assert.True(t, cleared)
		assert.False(t, draining(t, h1))
		res, err = s.p.MarkHostDraining(t.Context(), req)
		require.NoError(t, err)
		assert.False(t, res.Refused(req))
		assert.True(t, draining(t, h2))

		// Clearing a host that is not draining changes nothing
		cleared, err = s.p.ClearHostDraining(t.Context(), h1, mark.RollbackToken)
		require.NoError(t, err)
		assert.True(t, cleared)
		assert.False(t, draining(t, h1))

		// A host that isn't registered is not found
		_, err = s.p.ClearHostDraining(t.Context(), SpecHostNonExistent, mark.RollbackToken)
		require.ErrorIs(t, err, components.ErrHostUnregistered)
	})

	for _, transition := range []string{"competing mark", "accepted drain", "reattach"} {
		t.Run("rollback cannot clear a "+transition, func(t *testing.T) {
			err := s.p.Seed(t.Context(), Spec{})
			require.NoError(t, err)
			h1 := register(t, "10.3.6.1:5000", "MD-A")
			h2 := register(t, "10.3.6.2:5000", "MD-A")
			mark, err := s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: h1})
			require.NoError(t, err)
			require.NotEmpty(t, mark.RollbackToken)

			// Another request or the host itself takes ownership before the first request's rollback
			switch transition {
			case "competing mark":
				other, markErr := s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: h1})
				require.NoError(t, markErr)
				assert.True(t, other.AlreadyDraining)
				assert.Empty(t, other.RollbackToken)
			case "accepted drain":
				err = s.p.UpdateActorHost(t.Context(), h1, components.UpdateActorHostReq{Draining: true})
				require.NoError(t, err)
			case "reattach":
				_, err = s.p.RegisterHost(t.Context(), components.RegisterHostReq{ExistingHostID: h1, Address: "10.3.6.1:5000", ActorTypes: []components.ActorHostType{{ActorType: "MD-A"}}})
				require.NoError(t, err)
				// A new drain after reattaching must not be cleared by the old registration's token
				_, err = s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: h1})
				require.NoError(t, err)
			}

			cleared, err := s.p.ClearHostDraining(t.Context(), h1, mark.RollbackToken)
			require.NoError(t, err)
			assert.False(t, cleared)
			assert.True(t, draining(t, h1))
			last, err := s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: h2})
			require.NoError(t, err)
			assert.True(t, last.Refused(components.MarkHostDrainingReq{HostID: h2}))
		})
	}

	t.Run("a host with an expired registration is not found", func(t *testing.T) {
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		h1 := register(t, "10.3.2.1:5000", "MD-A")
		err = s.p.AdvanceClock(s.p.HealthCheckPolicy().Deadline() + time.Second)
		require.NoError(t, err)

		_, err = s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: h1})
		require.ErrorIs(t, err, components.ErrHostUnregistered)
		_, err = s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: SpecHostNonExistent})
		require.ErrorIs(t, err, components.ErrHostUnregistered)
		_, err = s.p.ClearHostDraining(t.Context(), h1, "missing-token")
		require.ErrorIs(t, err, components.ErrHostUnregistered)
	})

	t.Run("nothing is marked while an exclusive-access lease is held", func(t *testing.T) {
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		h1 := register(t, "10.3.5.1:5000", "MD-A")
		register(t, "10.3.5.2:5000", "MD-A")

		_, err = s.p.AcquireExclusiveLease(t.Context(), "md-owner", time.Minute)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.p.ReleaseExclusiveLease(context.WithoutCancel(t.Context()), "md-owner") })

		// Forced or not, the drain is refused and the host keeps serving
		_, err = s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: h1})
		require.ErrorIs(t, err, components.ErrClusterLocked)
		_, err = s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: h1, Force: true})
		require.ErrorIs(t, err, components.ErrClusterLocked)
		assert.False(t, draining(t, h1))

		// Once the lease is released the host can be marked
		err = s.p.ReleaseExclusiveLease(t.Context(), "md-owner")
		require.NoError(t, err)
		_, err = s.p.MarkHostDraining(t.Context(), components.MarkHostDrainingReq{HostID: h1})
		require.NoError(t, err)
		assert.True(t, draining(t, h1))
	})

	t.Run("concurrent drains never leave a type without a server", func(t *testing.T) {
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		// Two hosts are the only servers of a type, so at most one of them may be drained without force
		hosts := []string{
			register(t, "10.3.3.1:5000", "MD-A"),
			register(t, "10.3.3.2:5000", "MD-A"),
		}

		var (
			wg      sync.WaitGroup
			refused atomic.Int32
			errs    = make([]error, len(hosts))
		)
		for i, hostID := range hosts {
			wg.Go(func() {
				req := components.MarkHostDrainingReq{HostID: hostID}
				res, err := s.p.MarkHostDraining(t.Context(), req)
				errs[i] = err
				if err == nil && res.Refused(req) {
					refused.Add(1)
				}
			})
		}
		wg.Wait()

		err = errors.Join(errs...)
		require.NoError(t, err)
		assert.Equal(t, int32(1), refused.Load(), "exactly one of the two drains should be refused")
		assert.NotEqual(t, draining(t, hosts[0]), draining(t, hosts[1]), "exactly one host should be draining")
	})
}

// TestListPlacements covers ListPlacements
func (s Suite) TestListPlacements(t *testing.T) {
	// placementKeys renders a page as type/id@host strings, so ordering and filters can be asserted in one comparison
	placementKeys := func(placements []components.PlacementInfo) []string {
		keys := make([]string, len(placements))
		for i, p := range placements {
			keys[i] = p.ActorType + "/" + p.ActorID + "@" + p.HostID
		}
		return keys
	}

	// list returns the keys of one page
	list := func(t *testing.T, ctx context.Context, req components.ListPlacementsReq) ([]string, bool) {
		t.Helper()
		res, err := s.p.ListPlacements(ctx, req)
		require.NoError(t, err)
		return placementKeys(res.Placements), res.HasMore
	}

	ctx := t.Context()

	// H1 and H2 are live, and H5's health check is past the deadline, so its placements are about to be collected
	err := s.p.Seed(ctx, Spec{
		Hosts: HostSpecCollection{
			{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: time.Second},
			{HostID: SpecHostH2, Address: "127.0.0.1:4002", LastHealthAgo: time.Second},
			{HostID: SpecHostH5, Address: "127.0.0.1:4005", LastHealthAgo: 24 * time.Hour},
		},
		HostActorTypes: HostActorTypeSpecCollection{
			{HostID: SpecHostH1, ActorType: "PL-A", ActorIdleTimeout: 5 * time.Minute},
			{HostID: SpecHostH1, ActorType: "PL-B", ActorIdleTimeout: 3 * time.Minute},
			{HostID: SpecHostH2, ActorType: "PL-A", ActorIdleTimeout: 5 * time.Minute},
			{HostID: SpecHostH5, ActorType: "PL-A", ActorIdleTimeout: 5 * time.Minute},
		},
		ActiveActors: []ActiveActorSpec{
			{ActorType: "PL-B", ActorID: "b2", HostID: SpecHostH1, ActorIdleTimeout: 3 * time.Minute},
			{ActorType: "PL-A", ActorID: "a3", HostID: SpecHostH2, ActorIdleTimeout: 5 * time.Minute},
			{ActorType: "PL-A", ActorID: "a1", HostID: SpecHostH1, ActorIdleTimeout: 5 * time.Minute},
			{ActorType: "PL-A", ActorID: "a4", HostID: SpecHostH5, ActorIdleTimeout: 5 * time.Minute},
			{ActorType: "PL-B", ActorID: "b1", HostID: SpecHostH1, ActorIdleTimeout: 2 * time.Minute},
			{ActorType: "PL-A", ActorID: "a2", HostID: SpecHostH1, ActorIdleTimeout: 5 * time.Minute},
		},
	})
	require.NoError(t, err)

	// A draining host keeps its placements until it removes them, so they stay listed
	err = s.p.UpdateActorHost(ctx, SpecHostH2, components.UpdateActorHostReq{Draining: true})
	require.NoError(t, err)

	all := []string{
		"PL-A/a1@" + SpecHostH1,
		"PL-A/a2@" + SpecHostH1,
		"PL-A/a3@" + SpecHostH2,
		"PL-B/b1@" + SpecHostH1,
		"PL-B/b2@" + SpecHostH1,
	}

	t.Run("lists live placements ordered by actor type and ID", func(t *testing.T) {
		keys, hasMore := list(t, t.Context(), components.ListPlacementsReq{})
		assert.Equal(t, all, keys)
		assert.False(t, hasMore)
	})

	t.Run("returns the idle timeout of each placement", func(t *testing.T) {
		res, err := s.p.ListPlacements(t.Context(), components.ListPlacementsReq{ActorType: "PL-B"})
		require.NoError(t, err)
		require.Len(t, res.Placements, 2)
		assert.Equal(t, components.PlacementInfo{ActorType: "PL-B", ActorID: "b1", HostID: SpecHostH1, IdleTimeout: 2 * time.Minute}, res.Placements[0])
		assert.Equal(t, components.PlacementInfo{ActorType: "PL-B", ActorID: "b2", HostID: SpecHostH1, IdleTimeout: 3 * time.Minute}, res.Placements[1])
	})

	t.Run("filters by host, actor type, or both", func(t *testing.T) {
		ctx := t.Context()

		keys, _ := list(t, ctx, components.ListPlacementsReq{HostID: SpecHostH1})
		assert.Equal(t, []string{"PL-A/a1@" + SpecHostH1, "PL-A/a2@" + SpecHostH1, "PL-B/b1@" + SpecHostH1, "PL-B/b2@" + SpecHostH1}, keys)

		keys, _ = list(t, ctx, components.ListPlacementsReq{HostID: SpecHostH2})
		assert.Equal(t, []string{"PL-A/a3@" + SpecHostH2}, keys)

		keys, _ = list(t, ctx, components.ListPlacementsReq{ActorType: "PL-A"})
		assert.Equal(t, []string{"PL-A/a1@" + SpecHostH1, "PL-A/a2@" + SpecHostH1, "PL-A/a3@" + SpecHostH2}, keys)

		keys, _ = list(t, ctx, components.ListPlacementsReq{HostID: SpecHostH1, ActorType: "PL-A"})
		assert.Equal(t, []string{"PL-A/a1@" + SpecHostH1, "PL-A/a2@" + SpecHostH1}, keys)

		// Filters that match nothing return an empty page rather than an error
		keys, hasMore := list(t, ctx, components.ListPlacementsReq{ActorType: "PL-None"})
		assert.Empty(t, keys)
		assert.False(t, hasMore)

		keys, hasMore = list(t, ctx, components.ListPlacementsReq{HostID: SpecHostNonExistent})
		assert.Empty(t, keys)
		assert.False(t, hasMore)

		keys, _ = list(t, ctx, components.ListPlacementsReq{HostID: SpecHostH2, ActorType: "PL-B"})
		assert.Empty(t, keys)
	})

	t.Run("hides placements on an expired host", func(t *testing.T) {
		keys, _ := list(t, t.Context(), components.ListPlacementsReq{HostID: SpecHostH5})
		assert.Empty(t, keys)
	})

	t.Run("pages with the actor reference cursor", func(t *testing.T) {
		ctx := t.Context()

		// Walk the collection two at a time, using the last placement of each page as the cursor for the next one
		var (
			seen    []string
			hasMore []bool
			cursor  ref.ActorRef
		)
		for range all {
			res, err := s.p.ListPlacements(ctx, components.ListPlacementsReq{After: cursor, Limit: 2})
			require.NoError(t, err)
			require.NotEmpty(t, res.Placements)

			seen = append(seen, placementKeys(res.Placements)...)
			hasMore = append(hasMore, res.HasMore)
			last := res.Placements[len(res.Placements)-1]
			cursor = ref.NewActorRef(last.ActorType, last.ActorID)
			if !res.HasMore {
				break
			}
		}
		assert.Equal(t, all, seen)
		assert.Equal(t, []bool{true, true, false}, hasMore)

		// The cursor compares the actor ID only within the cursor's actor type, and carries over to the next type
		keys, _ := list(t, ctx, components.ListPlacementsReq{After: ref.NewActorRef("PL-A", "a1")})
		assert.Equal(t, all[1:], keys)
		keys, _ = list(t, ctx, components.ListPlacementsReq{After: ref.NewActorRef("PL-A", "zzz")})
		assert.Equal(t, all[3:], keys)
		keys, _ = list(t, ctx, components.ListPlacementsReq{After: ref.NewActorRef("PL-A", "a")})
		assert.Equal(t, all, keys)

		// The cursor combines with the filters
		keys, hasMore2 := list(t, ctx, components.ListPlacementsReq{HostID: SpecHostH1, After: ref.NewActorRef("PL-A", "a2"), Limit: 1})
		assert.Equal(t, []string{"PL-B/b1@" + SpecHostH1}, keys)
		assert.True(t, hasMore2)

		// A cursor at the end of the collection returns nothing
		keys, hasMore2 = list(t, ctx, components.ListPlacementsReq{After: ref.NewActorRef("PL-B", "b2")})
		assert.Empty(t, keys)
		assert.False(t, hasMore2)

		// A negative limit is served with the default page size rather than failing
		keys, hasMore2 = list(t, ctx, components.ListPlacementsReq{Limit: -1})
		assert.Equal(t, all, keys)
		assert.False(t, hasMore2)
	})

	t.Run("never removes a placement", func(t *testing.T) {
		// Listing must not garbage collect the placement on the expired host, which only the provider's collector removes
		spec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)

		found := slices.ContainsFunc(spec.ActiveActors, func(a ActiveActorSpec) bool {
			return a.ActorType == "PL-A" && a.ActorID == "a4"
		})
		assert.True(t, found, "the placement on the expired host must still be stored")
		assert.Len(t, spec.ActiveActors, 6)
	})
}

// TestQueryJobs covers QueryJobs
func (s Suite) TestQueryJobs(t *testing.T) {
	ctx := t.Context()
	err := s.p.Seed(ctx, Spec{})
	require.NoError(t, err)

	// The terminal-job store is not wiped by Seed, so every job here uses actor types no other test touches
	const (
		typeA = "QJ-A"
		typeB = "QJ-B"
	)

	host, err := s.p.RegisterHost(ctx, components.RegisterHostReq{
		Address: "10.3.0.1:5000",
		ActorTypes: []components.ActorHostType{
			{ActorType: typeA, IdleTimeout: 5 * time.Minute},
			{ActorType: typeB, IdleTimeout: 5 * time.Minute},
		},
	})
	require.NoError(t, err)

	now := s.p.Now()

	// dispatch creates a job and returns its ID
	dispatch := func(actorType string, actorID string, name string, due time.Time) string {
		t.Helper()
		jobID, _, _, err := s.p.DispatchJob(ctx, ref.NewAlarmRef(actorType, actorID, name), components.SetAlarmReq{
			AlarmProperties: ref.AlarmProperties{DueTime: due, Data: []byte("job-" + name)},
			Kind:            components.AlarmKindJob,
			JobMethod:       "method-" + name,
		})
		require.NoError(t, err)
		return jobID
	}

	// Dispatch the jobs interleaving live and terminal ones, so the union has to be merged by job ID
	pendingA1 := dispatch(typeA, "a1", "p1", now.Add(time.Hour))
	completedA3 := dispatch(typeA, "a3", "c1", now)
	activeA2 := dispatch(typeA, "a2", "act", now)
	pendingA1b := dispatch(typeA, "a1", "p2", now.Add(2*time.Hour))
	deadB1 := dispatch(typeB, "b1", "d1", now)
	expiredB2 := dispatch(typeB, "b2", "e1", now)
	pendingB1 := dispatch(typeB, "b1", "p3", now.Add(time.Hour))

	// A plain alarm lives in the same table but is never a job
	_, err = s.p.SetAlarm(ctx, ref.NewAlarmRef(typeA, "a1", "plain"), components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{DueTime: now.Add(time.Hour)},
		Kind:            components.AlarmKindAlarm,
	})
	require.NoError(t, err)

	// Lease the jobs that are due now, then finalize all but the active one
	leases, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{Hosts: []string{host.HostID}})
	require.NoError(t, err)
	leaseOf := func(jobID string) *ref.AlarmLease {
		t.Helper()
		idx := slices.IndexFunc(leases, func(l *ref.AlarmLease) bool { return l.Key() == jobID })
		require.GreaterOrEqualf(t, idx, 0, "job %s was not leased", jobID)
		return leases[idx]
	}
	err = s.p.CompleteJob(ctx, leaseOf(completedA3), components.CompleteJobReq{Attempts: 1, Retention: time.Hour})
	require.NoError(t, err)
	err = s.p.DeadLetterAlarm(ctx, leaseOf(deadB1), components.DeadLetterAlarmReq{Reason: "boom", Attempts: 2, Retention: time.Hour})
	require.NoError(t, err)
	err = s.p.CompleteJob(ctx, leaseOf(expiredB2), components.CompleteJobReq{Attempts: 1, Retention: time.Second})
	require.NoError(t, err)
	_ = leaseOf(activeA2)

	// Let the short retention elapse, which keeps the active job's lease valid
	err = s.p.AdvanceClock(2 * time.Second)
	require.NoError(t, err)

	statuses := map[string]components.JobStatus{
		pendingA1:   components.JobStatusPending,
		pendingA1b:  components.JobStatusPending,
		activeA2:    components.JobStatusActive,
		completedA3: components.JobStatusCompleted,
		deadB1:      components.JobStatusDeadLettered,
		pendingB1:   components.JobStatusPending,
	}

	// sortedIDs returns the given job IDs in the order QueryJobs must return them
	sortedIDs := func(ids ...string) []string {
		res := slices.Clone(ids)
		slices.Sort(res)
		return res
	}

	// jobIDs returns the IDs of one page
	jobIDs := func(jobs []components.JobInfo) []string {
		ids := make([]string, len(jobs))
		for i, j := range jobs {
			ids[i] = j.JobID
		}
		return ids
	}

	// query returns the job IDs of one page
	query := func(t *testing.T, req components.QueryJobsReq) ([]string, bool) {
		t.Helper()
		res, err := s.p.QueryJobs(t.Context(), req)
		require.NoError(t, err)
		return jobIDs(res.Jobs), res.HasMore
	}

	// queryAll walks every page of a query, asserting the pages are in strictly ascending order
	queryAll := func(t *testing.T, req components.QueryJobsReq) []components.JobInfo {
		t.Helper()
		var all []components.JobInfo
		for {
			res, err := s.p.QueryJobs(t.Context(), req)
			require.NoError(t, err)
			all = append(all, res.Jobs...)
			if !res.HasMore {
				break
			}
			require.NotEmpty(t, res.Jobs, "a page that reports more results must not be empty")
			req.After = uuid.MustParse(res.Jobs[len(res.Jobs)-1].JobID)
		}

		ids := jobIDs(all)
		assert.Truef(t, slices.IsSorted(ids), "jobs are not in job ID order: %v", ids)
		assert.Len(t, slices.Compact(slices.Clone(ids)), len(ids), "a job was returned twice")
		return all
	}

	t.Run("lists live and terminal jobs in job ID order", func(t *testing.T) {
		ids, hasMore := query(t, components.QueryJobsReq{ActorType: typeA})
		assert.Equal(t, sortedIDs(pendingA1, pendingA1b, activeA2, completedA3), ids)
		assert.False(t, hasMore)

		ids, hasMore = query(t, components.QueryJobsReq{ActorType: typeB})
		assert.Equal(t, sortedIDs(deadB1, pendingB1), ids, "an expired terminal job must be omitted")
		assert.False(t, hasMore)
	})

	t.Run("derives the status of each job", func(t *testing.T) {
		for _, actorType := range []string{typeA, typeB} {
			res, err := s.p.QueryJobs(t.Context(), components.QueryJobsReq{ActorType: actorType})
			require.NoError(t, err)
			for _, j := range res.Jobs {
				assert.Equalf(t, statuses[j.JobID], j.Status, "status of job %s", j.JobID)
			}
		}
	})

	t.Run("describes each job the way ListJobs does", func(t *testing.T) {
		all := append(queryAll(t, components.QueryJobsReq{ActorType: typeA}), queryAll(t, components.QueryJobsReq{ActorType: typeB})...)
		require.Len(t, all, len(statuses))

		for _, got := range all {
			jobs, err := s.p.ListJobs(t.Context(), got.ActorType, got.ActorID)
			require.NoError(t, err)
			idx := slices.IndexFunc(jobs, func(j components.JobInfo) bool { return j.JobID == got.JobID })
			require.GreaterOrEqualf(t, idx, 0, "job %s is not in ListJobs", got.JobID)
			want := jobs[idx]

			assertTimeNear(t, want.DueTime, got.DueTime, "due time of job %s", got.JobID)
			assertTimeNear(t, want.EndedAt, got.EndedAt, "end time of job %s", got.JobID)
			assertTimeNear(t, components.JobCreatedAt(got.JobID), got.CreatedAt, "creation time of job %s", got.JobID)
			want.DueTime, got.DueTime = time.Time{}, time.Time{}
			want.EndedAt, got.EndedAt = time.Time{}, time.Time{}
			want.CreatedAt, got.CreatedAt = time.Time{}, time.Time{}
			assert.Equalf(t, want, got, "job %s", got.JobID)
		}

		// Spot-check the fields of a terminal job, so a mismatch shared with ListJobs is still caught
		idx := slices.IndexFunc(all, func(j components.JobInfo) bool { return j.JobID == deadB1 })
		require.GreaterOrEqual(t, idx, 0)
		dead := all[idx]
		assert.Equal(t, typeB, dead.ActorType)
		assert.Equal(t, "b1", dead.ActorID)
		assert.Equal(t, "method-d1", dead.Method)
		assert.Equal(t, 2, dead.Attempts)
		assert.Equal(t, "boom", dead.LastError)
		assert.False(t, dead.EndedAt.IsZero())
	})

	t.Run("filters by actor and status", func(t *testing.T) {
		ids, _ := query(t, components.QueryJobsReq{ActorType: typeA, ActorID: "a1"})
		assert.Equal(t, sortedIDs(pendingA1, pendingA1b), ids)

		ids, _ = query(t, components.QueryJobsReq{ActorType: typeB, ActorID: "b1"})
		assert.Equal(t, sortedIDs(deadB1, pendingB1), ids)

		ids, _ = query(t, components.QueryJobsReq{ActorType: typeA, ActorID: "nobody"})
		assert.Empty(t, ids)

		ids, _ = query(t, components.QueryJobsReq{ActorType: typeA, Status: components.JobStatusPending})
		assert.Equal(t, sortedIDs(pendingA1, pendingA1b), ids)

		ids, _ = query(t, components.QueryJobsReq{ActorType: typeA, Status: components.JobStatusActive})
		assert.Equal(t, []string{activeA2}, ids)

		ids, _ = query(t, components.QueryJobsReq{ActorType: typeA, Status: components.JobStatusCompleted})
		assert.Equal(t, []string{completedA3}, ids)

		ids, _ = query(t, components.QueryJobsReq{ActorType: typeB, Status: components.JobStatusDeadLettered})
		assert.Equal(t, []string{deadB1}, ids)

		ids, _ = query(t, components.QueryJobsReq{ActorType: typeB, Status: components.JobStatusCompleted})
		assert.Empty(t, ids, "an expired terminal job must be omitted")

		ids, _ = query(t, components.QueryJobsReq{ActorType: typeB, ActorID: "b1", Status: components.JobStatusPending})
		assert.Equal(t, []string{pendingB1}, ids)

		ids, _ = query(t, components.QueryJobsReq{ActorType: "QJ-None"})
		assert.Empty(t, ids)
	})

	t.Run("an unknown status matches no job", func(t *testing.T) {
		// No job can be in a status the provider does not know, so this is an empty page rather than an error
		ids, hasMore := query(t, components.QueryJobsReq{Status: "bogus"})
		assert.Empty(t, ids)
		assert.False(t, hasMore)

		ids, hasMore = query(t, components.QueryJobsReq{ActorType: typeA, Status: "bogus"})
		assert.Empty(t, ids)
		assert.False(t, hasMore)
	})

	t.Run("ignores the actor ID without an actor type", func(t *testing.T) {
		// Other tests leave terminal jobs behind, so only check that every job of this test is included
		ids := jobIDs(queryAll(t, components.QueryJobsReq{ActorID: "a1", Limit: 3}))
		for id := range statuses {
			assert.Containsf(t, ids, id, "job %s is missing from the cluster-wide listing", id)
		}
		assert.NotContains(t, ids, expiredB2)
	})

	t.Run("lists every job across the cluster", func(t *testing.T) {
		ids := jobIDs(queryAll(t, components.QueryJobsReq{Limit: 2}))
		for id := range statuses {
			assert.Containsf(t, ids, id, "job %s is missing from the cluster-wide listing", id)
		}

		// A status filter without an actor type still applies
		all := queryAll(t, components.QueryJobsReq{Status: components.JobStatusActive})
		for _, j := range all {
			assert.Equal(t, components.JobStatusActive, j.Status)
		}
		assert.Contains(t, jobIDs(all), activeA2)
	})

	t.Run("pages across the union with the job ID cursor", func(t *testing.T) {
		want := sortedIDs(pendingA1, pendingA1b, activeA2, completedA3)

		for _, limit := range []int{1, 2, 3} {
			var (
				seen   []string
				cursor components.UUIDCursor
				pages  int
			)
			for {
				ids, hasMore := query(t, components.QueryJobsReq{ActorType: typeA, After: cursor, Limit: limit})
				require.LessOrEqual(t, len(ids), limit)
				seen = append(seen, ids...)
				pages++
				if !hasMore {
					break
				}
				require.NotEmpty(t, ids)
				cursor = uuid.MustParse(ids[len(ids)-1])
			}
			assert.Equalf(t, want, seen, "limit %d", limit)
			assert.Equalf(t, (len(want)+limit-1)/limit, pages, "limit %d must not return an empty trailing page", limit)
		}

		// The cursor is exclusive
		ids, _ := query(t, components.QueryJobsReq{ActorType: typeA, After: uuid.MustParse(want[1])})
		assert.Equal(t, want[2:], ids)
		ids, hasMore := query(t, components.QueryJobsReq{ActorType: typeA, After: uuid.MustParse(want[len(want)-1])})
		assert.Empty(t, ids)
		assert.False(t, hasMore)

		// The cursor does not need to be a job ID: the nil UUID sorts before every job and the max UUID after every job
		ids, hasMore = query(t, components.QueryJobsReq{ActorType: typeA, After: uuid.Nil()})
		assert.Equal(t, want, ids)
		assert.False(t, hasMore)
		ids, hasMore = query(t, components.QueryJobsReq{ActorType: typeA, After: uuid.Max()})
		assert.Empty(t, ids)
		assert.False(t, hasMore)

		// A negative limit is served with the default page size rather than failing
		ids, hasMore = query(t, components.QueryJobsReq{ActorType: typeA, Limit: -1})
		assert.Equal(t, want, ids)
		assert.False(t, hasMore)
	})

	t.Run("CountJobs counts the jobs QueryJobs lists, up to the limit", func(t *testing.T) {
		// The terminal-job store holds the jobs of other tests too, so each count is compared with a listing of the whole cluster
		for _, status := range []components.JobStatus{"", components.JobStatusPending, components.JobStatusActive, components.JobStatusCompleted, components.JobStatusDeadLettered} {
			total := len(queryAll(t, components.QueryJobsReq{Status: status, Limit: components.MaxManagementListLimit}))
			require.Positivef(t, total, "the test needs a job in status %q", status)

			n, err := s.p.CountJobs(t.Context(), components.CountJobsReq{Status: status, Limit: total + 10})
			require.NoError(t, err)
			assert.Equalf(t, total, n, "count of the jobs in status %q", status)

			// A limit at or below the total stops the count there, including when both halves of the union match
			n, err = s.p.CountJobs(t.Context(), components.CountJobsReq{Status: status, Limit: total})
			require.NoError(t, err)
			assert.Equalf(t, total, n, "count of the jobs in status %q at a limit equal to the total", status)
			n, err = s.p.CountJobs(t.Context(), components.CountJobsReq{Status: status, Limit: 1})
			require.NoError(t, err)
			assert.Equalf(t, 1, n, "count of the jobs in status %q at a limit of one", status)
		}

		// A limit that is not positive counts nothing, and no job is in an unknown status
		n, err := s.p.CountJobs(t.Context(), components.CountJobsReq{Limit: 0})
		require.NoError(t, err)
		assert.Zero(t, n)
		n, err = s.p.CountJobs(t.Context(), components.CountJobsReq{Status: "bogus", Limit: 10})
		require.NoError(t, err)
		assert.Zero(t, n)
	})

	t.Run("a live job whose lease expired is pending", func(t *testing.T) {
		// Past the lease duration the active job's lease is no longer valid, even though the lease ID is still stored
		err := s.p.AdvanceClock(2 * time.Minute)
		require.NoError(t, err)

		ids, _ := query(t, components.QueryJobsReq{ActorType: typeA, Status: components.JobStatusActive})
		assert.Empty(t, ids)

		ids, _ = query(t, components.QueryJobsReq{ActorType: typeA, Status: components.JobStatusPending})
		assert.Equal(t, sortedIDs(pendingA1, pendingA1b, activeA2), ids)

		// CountJobs derives the status the same way
		n, err := s.p.CountJobs(t.Context(), components.CountJobsReq{Status: components.JobStatusActive, Limit: 10})
		require.NoError(t, err)
		assert.Zero(t, n)
	})
}

// TestListAlarms covers ListAlarms
func (s Suite) TestListAlarms(t *testing.T) {
	ctx := t.Context()
	err := s.p.Seed(ctx, Spec{})
	require.NoError(t, err)

	host, err := s.p.RegisterHost(ctx, components.RegisterHostReq{
		Address: "10.4.0.1:5000",
		ActorTypes: []components.ActorHostType{
			{ActorType: "LA-A", IdleTimeout: 5 * time.Minute},
			{ActorType: "LA-B", IdleTimeout: 5 * time.Minute},
		},
	})
	require.NoError(t, err)

	now := s.p.Now()
	ttl := now.Add(24 * time.Hour)

	// setAlarm stores a plain alarm
	setAlarm := func(actorType string, actorID string, name string, props ref.AlarmProperties) {
		t.Helper()
		_, err := s.p.SetAlarm(ctx, ref.NewAlarmRef(actorType, actorID, name), components.SetAlarmReq{AlarmProperties: props, Kind: components.AlarmKindAlarm})
		require.NoError(t, err)
	}

	// Store the alarms out of order, so the listing has to sort them
	setAlarm("LA-B", "b1", "n1", ref.AlarmProperties{DueTime: now.Add(time.Hour)})
	setAlarm("LA-A", "a1", "n2", ref.AlarmProperties{DueTime: now.Add(2 * time.Hour), Interval: "PT5M", TTL: &ttl, Data: []byte("with-data")})
	setAlarm("LA-A", "a2", "n1", ref.AlarmProperties{DueTime: now.Add(time.Hour)})
	setAlarm("LA-A", "a1", "n1", ref.AlarmProperties{DueTime: now.Add(time.Hour)})
	setAlarm("LA-A", "a3", "due", ref.AlarmProperties{DueTime: now})

	// A job shares the table and must never be listed as an alarm
	_, _, _, err = s.p.DispatchJob(ctx, ref.NewAlarmRef("LA-A", "a1", "job"), components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{DueTime: now.Add(time.Hour)},
		Kind:            components.AlarmKindJob,
		JobMethod:       "process",
	})
	require.NoError(t, err)

	// Lease the alarm that is due, which also places its actor on the host
	leases, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{Hosts: []string{host.HostID}})
	require.NoError(t, err)
	require.Len(t, leases, 1)
	leased := leases[0]
	require.Equal(t, ref.NewAlarmRef("LA-A", "a3", "due"), leased.AlarmRef())
	leasedAt := s.p.Now()

	all := []string{"LA-A/a1/n1", "LA-A/a1/n2", "LA-A/a2/n1", "LA-A/a3/due", "LA-B/b1/n1"}

	// alarmKeys renders a page as type/id/name strings
	alarmKeys := func(alarms []components.AlarmInfo) []string {
		keys := make([]string, len(alarms))
		for i, a := range alarms {
			keys[i] = a.ActorType + "/" + a.ActorID + "/" + a.Name
		}
		return keys
	}

	// list returns the keys of one page
	list := func(t *testing.T, req components.ListAlarmsReq) ([]string, bool) {
		t.Helper()
		res, err := s.p.ListAlarms(t.Context(), req)
		require.NoError(t, err)
		return alarmKeys(res.Alarms), res.HasMore
	}

	// byKey indexes a full listing by type/id/name
	byKey := func(t *testing.T) map[string]components.AlarmInfo {
		t.Helper()
		res, err := s.p.ListAlarms(t.Context(), components.ListAlarmsReq{})
		require.NoError(t, err)
		out := make(map[string]components.AlarmInfo, len(res.Alarms))
		for _, a := range res.Alarms {
			out[a.ActorType+"/"+a.ActorID+"/"+a.Name] = a
		}
		return out
	}

	t.Run("lists plain alarms ordered by actor type, actor ID and name", func(t *testing.T) {
		keys, hasMore := list(t, components.ListAlarmsReq{})
		assert.Equal(t, all, keys)
		assert.False(t, hasMore)
	})

	t.Run("returns the properties of each alarm", func(t *testing.T) {
		alarms := byKey(t)
		require.Len(t, alarms, len(all))

		// Alarm IDs are unique
		ids := make(map[string]struct{}, len(alarms))
		for k, a := range alarms {
			assert.NotEmptyf(t, a.AlarmID, "alarm %s has no ID", k)
			ids[a.AlarmID] = struct{}{}
		}
		assert.Len(t, ids, len(alarms))

		full := alarms["LA-A/a1/n2"]
		assertTimeNear(t, now.Add(2*time.Hour), full.DueTime)
		assert.Equal(t, "PT5M", full.Interval)
		assertTimePtrNear(t, &ttl, full.TTL, "ttl")
		assert.Nil(t, full.LeaseExpiration)
		assert.Empty(t, full.LeaseHostID)

		minimal := alarms["LA-B/b1/n1"]
		assertTimeNear(t, now.Add(time.Hour), minimal.DueTime)
		assert.Empty(t, minimal.Interval)
		assert.Nil(t, minimal.TTL)
	})

	t.Run("reports the lease and the host of a leased alarm", func(t *testing.T) {
		got := byKey(t)["LA-A/a3/due"]
		assert.Equal(t, leased.Key(), got.AlarmID)
		assert.Equal(t, host.HostID, got.LeaseHostID)
		if assert.NotNil(t, got.LeaseExpiration) {
			assert.True(t, got.LeaseExpiration.After(leasedAt), "the lease must expire in the future")
			assert.WithinDuration(t, leasedAt.Add(GetProviderConfig().AlarmsLeaseDuration), *got.LeaseExpiration, time.Second)
		}

		// None of the other alarms is leased
		for k, a := range byKey(t) {
			if k == "LA-A/a3/due" {
				continue
			}
			assert.Nilf(t, a.LeaseExpiration, "alarm %s", k)
			assert.Emptyf(t, a.LeaseHostID, "alarm %s", k)
		}
	})

	t.Run("filters by actor type and actor", func(t *testing.T) {
		keys, _ := list(t, components.ListAlarmsReq{ActorType: "LA-A"})
		assert.Equal(t, all[:4], keys)

		keys, _ = list(t, components.ListAlarmsReq{ActorType: "LA-A", ActorID: "a1"})
		assert.Equal(t, []string{"LA-A/a1/n1", "LA-A/a1/n2"}, keys)

		// Filters that match nothing return an empty page rather than an error
		keys, _ = list(t, components.ListAlarmsReq{ActorType: "LA-B", ActorID: "a1"})
		assert.Empty(t, keys)

		keys, hasMore := list(t, components.ListAlarmsReq{ActorType: "LA-None"})
		assert.Empty(t, keys)
		assert.False(t, hasMore)

		// An actor ID without an actor type is ignored
		keys, _ = list(t, components.ListAlarmsReq{ActorID: "a1"})
		assert.Equal(t, all, keys)
	})

	t.Run("pages with the alarm reference cursor", func(t *testing.T) {
		var (
			seen    []string
			hasMore []bool
			cursor  ref.AlarmRef
		)
		for range all {
			res, err := s.p.ListAlarms(t.Context(), components.ListAlarmsReq{After: cursor, Limit: 2})
			require.NoError(t, err)
			require.NotEmpty(t, res.Alarms)

			seen = append(seen, alarmKeys(res.Alarms)...)
			hasMore = append(hasMore, res.HasMore)
			last := res.Alarms[len(res.Alarms)-1]
			cursor = ref.NewAlarmRef(last.ActorType, last.ActorID, last.Name)
			if !res.HasMore {
				break
			}
		}
		assert.Equal(t, all, seen)
		assert.Equal(t, []bool{true, true, false}, hasMore)

		// Each part of the cursor only matters when the parts before it are equal
		keys, _ := list(t, components.ListAlarmsReq{After: ref.NewAlarmRef("LA-A", "a1", "n1")})
		assert.Equal(t, all[1:], keys)
		keys, _ = list(t, components.ListAlarmsReq{After: ref.NewAlarmRef("LA-A", "a1", "zzz")})
		assert.Equal(t, all[2:], keys)
		keys, _ = list(t, components.ListAlarmsReq{After: ref.NewAlarmRef("LA-A", "zzz", "")})
		assert.Equal(t, all[4:], keys)

		// The cursor combines with the filters
		keys, more := list(t, components.ListAlarmsReq{ActorType: "LA-A", After: ref.NewAlarmRef("LA-A", "a1", "n2"), Limit: 1})
		assert.Equal(t, []string{"LA-A/a2/n1"}, keys)
		assert.True(t, more)

		// A cursor at the end returns nothing
		keys, more = list(t, components.ListAlarmsReq{After: ref.NewAlarmRef("LA-B", "b1", "n1")})
		assert.Empty(t, keys)
		assert.False(t, more)

		// A negative limit is served with the default page size rather than failing
		keys, more = list(t, components.ListAlarmsReq{Limit: -1})
		assert.Equal(t, all, keys)
		assert.False(t, more)
	})

	t.Run("an expired lease is not reported", func(t *testing.T) {
		err := s.p.AdvanceClock(2 * time.Minute)
		require.NoError(t, err)

		got := byKey(t)["LA-A/a3/due"]
		assert.Nil(t, got.LeaseExpiration)
		assert.Empty(t, got.LeaseHostID)
	})

	t.Run("a live lease on an expired host reports no host", func(t *testing.T) {
		// The expired host's placement is still stored, as it is until the garbage collector removes it
		err := s.p.Seed(t.Context(), Spec{
			Hosts: HostSpecCollection{
				{HostID: SpecHostH1, Address: "10.4.1.1:5000", LastHealthAgo: 2 * time.Second},
				{HostID: SpecHostH5, Address: "10.4.1.5:5000", LastHealthAgo: 24 * time.Hour},
			},
			HostActorTypes: HostActorTypeSpecCollection{
				{HostID: SpecHostH1, ActorType: "LA-A", ActorIdleTimeout: 5 * time.Minute},
				{HostID: SpecHostH5, ActorType: "LA-A", ActorIdleTimeout: 5 * time.Minute},
			},
			ActiveActors: []ActiveActorSpec{
				{ActorType: "LA-A", ActorID: "live", HostID: SpecHostH1, ActorIdleTimeout: 5 * time.Minute},
				{ActorType: "LA-A", ActorID: "gone", HostID: SpecHostH5, ActorIdleTimeout: 5 * time.Minute},
			},
			Alarms: []AlarmSpec{
				{AlarmID: "AA000000-00A1-4000-8000-000000000001", ActorType: "LA-A", ActorID: "live", Name: "n", LeaseTTL: new(time.Minute)},
				{AlarmID: "AA000000-00A1-4000-8000-000000000002", ActorType: "LA-A", ActorID: "gone", Name: "n", LeaseTTL: new(time.Minute)},
			},
		})
		require.NoError(t, err)

		alarms := byKey(t)
		live := alarms["LA-A/live/n"]
		assert.Equal(t, SpecHostH1, live.LeaseHostID)
		assert.NotNil(t, live.LeaseExpiration)

		// The lease itself is still valid, so its expiration is reported without a host
		gone := alarms["LA-A/gone/n"]
		assert.Empty(t, gone.LeaseHostID)
		assert.NotNil(t, gone.LeaseExpiration)
	})
}

// TestListStateActorTypes covers ListStateActorTypes
func (s Suite) TestListStateActorTypes(t *testing.T) {
	ctx := t.Context()
	err := s.p.Seed(ctx, Spec{})
	require.NoError(t, err)

	// setState stores state for an actor
	setState := func(actorType string, actorID string, ttl time.Duration) {
		t.Helper()
		err := s.p.SetState(ctx, ref.NewActorRef(actorType, actorID), []byte("data"), components.SetStateOpts{TTL: ttl})
		require.NoError(t, err)
	}

	// The characters of a LIKE pattern appear in types, so a prefix that is not escaped matches too much
	setState("wf.orders", "a1", 0)
	setState("wf.orders", "a2", 0)
	setState("wf.orders", "a3", time.Second)
	setState("wf.orders.registry", "r1", 0)
	setState("wf.payments", "p1", 0)
	setState("wf_x", "x1", 0)
	setState("wfZ", "z1", 0)
	setState("wf%", "pct1", 0)
	setState("other", "o1", 0)
	setState("wf.expiring", "e1", time.Second)
	setState("wf.deleted", "d1", 0)
	err = s.p.DeleteState(ctx, ref.NewActorRef("wf.deleted", "d1"))
	require.NoError(t, err)

	// list returns the actor types for a prefix
	list := func(t *testing.T, prefix string) []string {
		t.Helper()
		res, err := s.p.ListStateActorTypes(t.Context(), prefix)
		require.NoError(t, err)
		return res
	}

	t.Run("an empty prefix lists every type once in ascending order", func(t *testing.T) {
		assert.Equal(t, []string{"other", "wf%", "wf.expiring", "wf.orders", "wf.orders.registry", "wf.payments", "wfZ", "wf_x"}, list(t, ""))
	})

	t.Run("a prefix restricts the types", func(t *testing.T) {
		assert.Equal(t, []string{"wf.expiring", "wf.orders", "wf.orders.registry", "wf.payments"}, list(t, "wf."))
		assert.Equal(t, []string{"wf.orders", "wf.orders.registry"}, list(t, "wf.orders"))
		assert.Equal(t, []string{"wf.orders.registry"}, list(t, "wf.orders."))
		assert.Equal(t, []string{"other"}, list(t, "o"))
		assert.Empty(t, list(t, "nothing"))
		assert.Empty(t, list(t, "wf.orders.registry.more"))
	})

	t.Run("wildcard characters in the prefix are matched literally", func(t *testing.T) {
		assert.Equal(t, []string{"wf_x"}, list(t, "wf_"))
		assert.Equal(t, []string{"wf%"}, list(t, "wf%"))
		assert.Empty(t, list(t, "%"))
		assert.Empty(t, list(t, "_"))
	})

	t.Run("types whose state all expired are omitted before garbage collection", func(t *testing.T) {
		err := s.p.AdvanceClock(2 * time.Second)
		require.NoError(t, err)

		// wf.orders still has live rows, while wf.expiring has none left
		assert.Equal(t, []string{"wf.orders", "wf.orders.registry", "wf.payments"}, list(t, "wf."))
	})
}

// TestListStatesManagement covers the workflow labels ListStates returns and its creation-time range filter
func (s Suite) TestListStatesManagement(t *testing.T) {
	ctx := t.Context()
	err := s.p.Seed(ctx, Spec{})
	require.NoError(t, err)

	const actorType = "LSC"
	t0 := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

	// labelsFor builds labels with a creation time
	labelsFor := func(status string, created time.Time) *components.WorkflowLabels {
		return &components.WorkflowLabels{Status: status, Version: 1, Created: components.FormatWorkflowCreated(created)}
	}

	// The actor IDs do not sort in creation order, so the result must be ordered by actor ID rather than by creation time
	rows := map[string]*components.WorkflowLabels{
		"id-1": labelsFor("running", t0.Add(3*time.Hour)),
		"id-2": labelsFor("running", t0),
		"id-3": labelsFor("done", t0.Add(time.Hour)),
		"id-4": labelsFor("running", t0.Add(2*time.Hour)),
		"id-5": nil,
		"id-6": {Status: "running", Version: 1},
		"id-7": labelsFor("running", t0.Add(time.Hour+500*time.Millisecond)),
		"id-8": {Status: "done", Version: 2, Parent: "id-1", Created: components.FormatWorkflowCreated(t0.Add(30 * time.Minute))},
	}
	for id, labels := range rows {
		err := s.p.SetState(ctx, ref.NewActorRef(actorType, id), []byte("data-"+id), components.SetStateOpts{WorkflowLabels: labels})
		require.NoError(t, err)
	}

	// listIDs returns the actor IDs of one page
	listIDs := func(t *testing.T, req components.ListStatesReq) ([]string, bool) {
		t.Helper()
		req.ActorType = actorType
		res, err := s.p.ListStates(t.Context(), req)
		require.NoError(t, err)
		ids := make([]string, len(res.States))
		for i, st := range res.States {
			ids[i] = st.ActorID
		}
		return ids, res.HasMore
	}

	t.Run("returns the stored labels of every row", func(t *testing.T) {
		for _, includeData := range []bool{false, true} {
			res, err := s.p.ListStates(t.Context(), components.ListStatesReq{ActorType: actorType, IncludeData: includeData})
			require.NoError(t, err)
			require.Len(t, res.States, len(rows))

			for _, st := range res.States {
				assert.Equalf(t, rows[st.ActorID], st.WorkflowLabels, "labels of %s (include data %v)", st.ActorID, includeData)
				if includeData {
					assert.Equal(t, "data-"+st.ActorID, string(st.Data))
				} else {
					assert.Empty(t, st.Data)
				}
			}
		}

		// Filtered listings return the labels too
		res, err := s.p.ListStates(t.Context(), components.ListStatesReq{ActorType: actorType, WorkflowLabels: &components.WorkflowLabels{Parent: "id-1"}})
		require.NoError(t, err)
		require.Len(t, res.States, 1)
		assert.Equal(t, rows["id-8"], res.States[0].WorkflowLabels)
	})

	t.Run("filters on a half-open creation range", func(t *testing.T) {
		ids, _ := listIDs(t, components.ListStatesReq{CreatedFrom: t0.Add(time.Hour), CreatedTo: t0.Add(3 * time.Hour)})
		assert.Equal(t, []string{"id-3", "id-4", "id-7"}, ids)

		ids, _ = listIDs(t, components.ListStatesReq{CreatedFrom: t0.Add(2 * time.Hour)})
		assert.Equal(t, []string{"id-1", "id-4"}, ids)

		ids, _ = listIDs(t, components.ListStatesReq{CreatedTo: t0.Add(time.Hour)})
		assert.Equal(t, []string{"id-2", "id-8"}, ids)

		// An empty or inverted range matches nothing rather than failing
		ids, _ = listIDs(t, components.ListStatesReq{CreatedFrom: t0.Add(time.Hour), CreatedTo: t0.Add(time.Hour)})
		assert.Empty(t, ids)

		ids, _ = listIDs(t, components.ListStatesReq{CreatedFrom: t0.Add(3 * time.Hour), CreatedTo: t0})
		assert.Empty(t, ids)

		// Sub-second creation times are compared at full precision
		ids, _ = listIDs(t, components.ListStatesReq{CreatedFrom: t0.Add(time.Hour + time.Millisecond), CreatedTo: t0.Add(2 * time.Hour)})
		assert.Equal(t, []string{"id-7"}, ids)
	})

	t.Run("rows without a creation time are excluded by a range", func(t *testing.T) {
		ids, _ := listIDs(t, components.ListStatesReq{CreatedFrom: t0.Add(-time.Hour)})
		assert.Equal(t, []string{"id-1", "id-2", "id-3", "id-4", "id-7", "id-8"}, ids)
	})

	t.Run("the range is compared in UTC whatever the location of the bounds", func(t *testing.T) {
		loc := time.FixedZone("minus0800", -8*60*60)
		ids, _ := listIDs(t, components.ListStatesReq{CreatedFrom: t0.Add(time.Hour).In(loc), CreatedTo: t0.Add(3 * time.Hour).In(loc)})
		assert.Equal(t, []string{"id-3", "id-4", "id-7"}, ids)
	})

	t.Run("the range combines with the label filter", func(t *testing.T) {
		ids, _ := listIDs(t, components.ListStatesReq{
			WorkflowLabels: &components.WorkflowLabels{Status: "running"},
			CreatedFrom:    t0.Add(time.Hour),
			CreatedTo:      t0.Add(3 * time.Hour),
		})
		assert.Equal(t, []string{"id-4", "id-7"}, ids)
	})

	t.Run("the created label is not an equality filter", func(t *testing.T) {
		ids, _ := listIDs(t, components.ListStatesReq{WorkflowLabels: &components.WorkflowLabels{Status: "running", Created: "not-a-time"}})
		assert.Equal(t, []string{"id-1", "id-2", "id-4", "id-6", "id-7"}, ids)
	})

	t.Run("a range-filtered listing pages in actor-ID order", func(t *testing.T) {
		var (
			seen   []string
			cursor string
		)
		for {
			ids, hasMore := listIDs(t, components.ListStatesReq{CreatedFrom: t0, After: cursor, Limit: 2})
			seen = append(seen, ids...)
			if !hasMore {
				break
			}
			require.NotEmpty(t, ids)
			cursor = ids[len(ids)-1]
		}
		assert.Equal(t, []string{"id-1", "id-2", "id-3", "id-4", "id-7", "id-8"}, seen)
	})

	t.Run("CountStates counts the rows ListStates lists, up to the limit", func(t *testing.T) {
		// count counts the rows of the test's actor type
		count := func(t *testing.T, labels *components.WorkflowLabels, limit int) int {
			t.Helper()
			n, err := s.p.CountStates(t.Context(), components.CountStatesReq{ActorType: actorType, WorkflowLabels: labels, Limit: limit})
			require.NoError(t, err)
			return n
		}

		assert.Equal(t, len(rows), count(t, nil, 100))
		assert.Equal(t, 5, count(t, &components.WorkflowLabels{Status: "running"}, 100))
		assert.Equal(t, 2, count(t, &components.WorkflowLabels{Status: "done"}, 100))
		assert.Equal(t, 1, count(t, &components.WorkflowLabels{Status: "done", Version: 2}, 100))
		assert.Zero(t, count(t, &components.WorkflowLabels{Status: "unknown"}, 100))

		// The created label is not an equality filter, as in ListStates
		assert.Equal(t, 5, count(t, &components.WorkflowLabels{Status: "running", Created: "not-a-time"}, 100))

		// The count stops at the limit, and a limit that is not positive counts nothing
		assert.Equal(t, 3, count(t, &components.WorkflowLabels{Status: "running"}, 3))
		assert.Equal(t, 5, count(t, &components.WorkflowLabels{Status: "running"}, 5))
		assert.Zero(t, count(t, &components.WorkflowLabels{Status: "running"}, 0))

		// Other actor types are not counted
		n, err := s.p.CountStates(t.Context(), components.CountStatesReq{ActorType: actorType + "-other", Limit: 100})
		require.NoError(t, err)
		assert.Zero(t, n)
	})

	t.Run("CountStates skips expired state before garbage collection", func(t *testing.T) {
		const expiringType = "LSC-EXP"
		err := s.p.SetState(t.Context(), ref.NewActorRef(expiringType, "short"), []byte("x"), components.SetStateOpts{TTL: time.Second, WorkflowLabels: &components.WorkflowLabels{Status: "running"}})
		require.NoError(t, err)
		err = s.p.SetState(t.Context(), ref.NewActorRef(expiringType, "long"), []byte("x"), components.SetStateOpts{TTL: time.Hour, WorkflowLabels: &components.WorkflowLabels{Status: "running"}})
		require.NoError(t, err)

		err = s.p.AdvanceClock(2 * time.Second)
		require.NoError(t, err)

		n, err := s.p.CountStates(t.Context(), components.CountStatesReq{ActorType: expiringType, WorkflowLabels: &components.WorkflowLabels{Status: "running"}, Limit: 100})
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})
}

// TestWorkflowEvents covers SetStateOpts.AppendEvents and ListWorkflowEvents
func (s Suite) TestWorkflowEvents(t *testing.T) {
	const actorType = "WFE"

	// makeEvents builds events with sequence numbers from..to, whose data names the batch that wrote them
	makeEvents := func(from int64, to int64, batch string) []components.WorkflowEvent {
		base := s.p.Now().Truncate(time.Millisecond)
		res := make([]components.WorkflowEvent, 0, to-from+1)
		for seq := from; seq <= to; seq++ {
			res = append(res, components.WorkflowEvent{
				Seq:  seq,
				Time: base.Add(time.Duration(seq) * time.Second),
				Kind: fmt.Sprintf("kind-%d", seq),
				Data: fmt.Appendf(nil, "%s-%d", batch, seq),
			})
		}
		return res
	}

	// setState writes state for an actor, appending the given events
	setState := func(t *testing.T, actorID string, opts components.SetStateOpts) {
		t.Helper()
		err := s.p.SetState(t.Context(), ref.NewActorRef(actorType, actorID), []byte("state-"+actorID), opts)
		require.NoError(t, err)
	}

	// listAll returns every event of an actor in one page
	listAll := func(t *testing.T, actorType string, actorID string) []components.WorkflowEvent {
		t.Helper()
		res, err := s.p.ListWorkflowEvents(t.Context(), components.ListWorkflowEventsReq{ActorType: actorType, ActorID: actorID, Limit: components.MaxManagementListLimit})
		require.NoError(t, err)
		assert.False(t, res.HasMore)
		return res.Events
	}

	// assertEvents asserts that an actor's events are exactly the given ones, in order
	assertEvents := func(t *testing.T, want []components.WorkflowEvent, got []components.WorkflowEvent) {
		t.Helper()
		require.Len(t, got, len(want))
		for i := range want {
			assert.Equalf(t, want[i].Seq, got[i].Seq, "event %d seq", i)
			assert.Equalf(t, want[i].Kind, got[i].Kind, "event %d kind", i)
			assert.Truef(t, bytes.Equal(want[i].Data, got[i].Data), "event %d data: want %q got %q", i, want[i].Data, got[i].Data)
			assertTimeNear(t, want[i].Time, got[i].Time, "event %d time", i)
		}
	}

	err := s.p.Seed(t.Context(), Spec{})
	require.NoError(t, err)

	t.Run("appends events with the state", func(t *testing.T) {
		first := makeEvents(1, 3, "first")

		// An event may carry no data at all
		first[1].Data = nil
		setState(t, "append", components.SetStateOpts{AppendEvents: first})
		assertEvents(t, first, listAll(t, actorType, "append"))

		// A later write appends to the history
		second := makeEvents(4, 5, "second")
		setState(t, "append", components.SetStateOpts{AppendEvents: second})
		assertEvents(t, append(slices.Clone(first), second...), listAll(t, actorType, "append"))

		// A write without events leaves the history alone
		setState(t, "append", components.SetStateOpts{})
		assertEvents(t, append(slices.Clone(first), second...), listAll(t, actorType, "append"))

		// The state row itself is written as usual
		data, err := s.p.GetState(t.Context(), ref.NewActorRef(actorType, "append"))
		require.NoError(t, err)
		assert.Equal(t, []byte("state-append"), data)
	})

	t.Run("an event whose sequence number exists is ignored", func(t *testing.T) {
		original := makeEvents(1, 3, "original")
		setState(t, "conflict", components.SetStateOpts{AppendEvents: original})

		// A retried write repeats events the first attempt stored, next to new ones
		retry := makeEvents(2, 4, "retry")
		setState(t, "conflict", components.SetStateOpts{AppendEvents: retry})

		assertEvents(t, append(slices.Clone(original), retry[2]), listAll(t, actorType, "conflict"))
	})

	t.Run("a history restarting at sequence one replaces the previous one", func(t *testing.T) {
		setState(t, "reset", components.SetStateOpts{AppendEvents: makeEvents(1, 5, "old")})

		// A new instance reusing the actor ID starts its history over
		fresh := makeEvents(1, 2, "new")
		setState(t, "reset", components.SetStateOpts{AppendEvents: fresh})
		assertEvents(t, fresh, listAll(t, actorType, "reset"))
	})

	t.Run("histories are kept per actor", func(t *testing.T) {
		mine := makeEvents(1, 2, "mine")
		setState(t, "isolated", components.SetStateOpts{AppendEvents: mine})

		// The same actor ID under another type, and another actor of the same type, each have their own history
		err := s.p.SetState(t.Context(), ref.NewActorRef(actorType+"-other", "isolated"), []byte("x"), components.SetStateOpts{AppendEvents: makeEvents(1, 3, "other-type")})
		require.NoError(t, err)
		setState(t, "isolated-2", components.SetStateOpts{AppendEvents: makeEvents(1, 1, "other-actor")})

		// Resetting one history must not touch the others
		setState(t, "isolated-2", components.SetStateOpts{AppendEvents: makeEvents(1, 1, "other-actor-reset")})

		assertEvents(t, mine, listAll(t, actorType, "isolated"))
		assert.Len(t, listAll(t, actorType+"-other", "isolated"), 3)

		// An actor with no state, and one whose state has no history, both have no events
		assert.Empty(t, listAll(t, actorType, "never-written"))
		setState(t, "no-history", components.SetStateOpts{})
		assert.Empty(t, listAll(t, actorType, "no-history"))
	})

	t.Run("deleting the state deletes its events", func(t *testing.T) {
		setState(t, "deleted", components.SetStateOpts{AppendEvents: makeEvents(1, 3, "gone")})
		err := s.p.DeleteState(t.Context(), ref.NewActorRef(actorType, "deleted"))
		require.NoError(t, err)
		assert.Empty(t, listAll(t, actorType, "deleted"))

		// Writing the state again without a history must not resurrect the old events, so they have to be gone rather than hidden
		setState(t, "deleted", components.SetStateOpts{})
		assert.Empty(t, listAll(t, actorType, "deleted"))
	})

	t.Run("events of expired state are hidden and then collected with it", func(t *testing.T) {
		setState(t, "expiring", components.SetStateOpts{TTL: time.Second, AppendEvents: makeEvents(1, 3, "expiring")})
		setState(t, "lasting", components.SetStateOpts{TTL: time.Hour, AppendEvents: makeEvents(1, 2, "lasting")})
		assert.Len(t, listAll(t, actorType, "expiring"), 3)

		// The expired row is still stored at this point, so this asserts the listing checks the expiration
		err := s.p.AdvanceClock(2 * time.Second)
		require.NoError(t, err)
		assert.Empty(t, listAll(t, actorType, "expiring"))
		assert.Len(t, listAll(t, actorType, "lasting"), 2)

		// Collecting the expired state deletes its events, so a new state under the same ID starts with no history
		err = s.p.CleanupExpired(t.Context())
		require.NoError(t, err)
		setState(t, "expiring", components.SetStateOpts{})
		assert.Empty(t, listAll(t, actorType, "expiring"))
		assert.Len(t, listAll(t, actorType, "lasting"), 2)
	})

	t.Run("a job's initial state that replaces an expired state removes its events", func(t *testing.T) {
		// dispatch dispatches a job that stores an initial state for the actor
		dispatch := func(t *testing.T, actorID string) {
			t.Helper()
			_, _, _, err := s.p.DispatchJob(t.Context(), ref.NewAlarmRef(actorType, actorID, "start"), components.SetAlarmReq{
				AlarmProperties: ref.AlarmProperties{DueTime: s.p.Now().Add(time.Hour)},
				Kind:            components.AlarmKindJob,
				JobMethod:       "start",
				InitialState:    &components.InitialState{Data: []byte("placeholder"), WorkflowLabels: &components.WorkflowLabels{Status: "pending"}},
			})
			require.NoError(t, err)
		}

		// The expired row is still stored when the initial state replaces it, and its events must not become the new state's history
		setState(t, "reused", components.SetStateOpts{TTL: time.Second, AppendEvents: makeEvents(1, 3, "reused")})
		err := s.p.AdvanceClock(2 * time.Second)
		require.NoError(t, err)
		dispatch(t, "reused")
		assert.Empty(t, listAll(t, actorType, "reused"))

		// An initial state that leaves live state in place leaves its events alone too
		live := makeEvents(1, 2, "live")
		setState(t, "live", components.SetStateOpts{TTL: time.Hour, AppendEvents: live})
		dispatch(t, "live")
		assertEvents(t, live, listAll(t, actorType, "live"))
	})

	t.Run("pages with the sequence cursor", func(t *testing.T) {
		all := makeEvents(1, 5, "paged")
		setState(t, "paged", components.SetStateOpts{AppendEvents: all})

		var (
			seen    []components.WorkflowEvent
			hasMore []bool
			cursor  int64
		)
		for range all {
			res, err := s.p.ListWorkflowEvents(t.Context(), components.ListWorkflowEventsReq{ActorType: actorType, ActorID: "paged", AfterSeq: cursor, Limit: 2})
			require.NoError(t, err)
			require.NotEmpty(t, res.Events)

			seen = append(seen, res.Events...)
			hasMore = append(hasMore, res.HasMore)
			cursor = res.Events[len(res.Events)-1].Seq
			if !res.HasMore {
				break
			}
		}
		assertEvents(t, all, seen)
		assert.Equal(t, []bool{true, true, false}, hasMore)

		// A cursor at the end returns nothing
		res, err := s.p.ListWorkflowEvents(t.Context(), components.ListWorkflowEventsReq{ActorType: actorType, ActorID: "paged", AfterSeq: 5})
		require.NoError(t, err)
		assert.Empty(t, res.Events)
		assert.False(t, res.HasMore)

		// A negative cursor starts from the beginning
		res, err = s.p.ListWorkflowEvents(t.Context(), components.ListWorkflowEventsReq{ActorType: actorType, ActorID: "paged", AfterSeq: -1})
		require.NoError(t, err)
		assertEvents(t, all, res.Events)
	})

	t.Run("limits the page to the default when no limit is requested", func(t *testing.T) {
		count := int64(components.DefaultManagementListLimit + 1)
		setState(t, "many", components.SetStateOpts{AppendEvents: makeEvents(1, count, "many")})

		res, err := s.p.ListWorkflowEvents(t.Context(), components.ListWorkflowEventsReq{ActorType: actorType, ActorID: "many"})
		require.NoError(t, err)
		require.Len(t, res.Events, components.DefaultManagementListLimit)
		assert.True(t, res.HasMore)
		assert.Equal(t, int64(components.DefaultManagementListLimit), res.Events[len(res.Events)-1].Seq)

		// A negative limit is served with the default page size too
		res, err = s.p.ListWorkflowEvents(t.Context(), components.ListWorkflowEventsReq{ActorType: actorType, ActorID: "many", Limit: -1})
		require.NoError(t, err)
		assert.Len(t, res.Events, components.DefaultManagementListLimit)
		assert.True(t, res.HasMore)

		// Asking for more than the maximum is served with the capped page size rather than failing
		res, err = s.p.ListWorkflowEvents(t.Context(), components.ListWorkflowEventsReq{ActorType: actorType, ActorID: "many", Limit: components.MaxManagementListLimit + 1})
		require.NoError(t, err)
		assert.Len(t, res.Events, int(count))
		assert.False(t, res.HasMore)
	})

	t.Run("appends a wide fan-out in a single write", func(t *testing.T) {
		// More events than fit in one SQLite statement, which limits the number of bound parameters
		const count = 6000
		setState(t, "wide", components.SetStateOpts{AppendEvents: makeEvents(1, count, "wide")})

		// A retry that repeats the whole batch and adds one more event only stores the new one
		setState(t, "wide", components.SetStateOpts{AppendEvents: makeEvents(2, count+1, "wide-retry")})

		// Walk every page, checking that the sequence has no gaps and that the first write won
		var (
			after int64
			total int
		)
		for {
			res, err := s.p.ListWorkflowEvents(t.Context(), components.ListWorkflowEventsReq{ActorType: actorType, ActorID: "wide", AfterSeq: after, Limit: components.MaxManagementListLimit})
			require.NoError(t, err)
			for _, ev := range res.Events {
				after++
				require.Equal(t, after, ev.Seq)
				batch := "wide"
				if ev.Seq == count+1 {
					batch = "wide-retry"
				}
				require.Equal(t, fmt.Sprintf("%s-%d", batch, ev.Seq), string(ev.Data))
			}
			total += len(res.Events)
			if !res.HasMore {
				break
			}
		}
		assert.Equal(t, count+1, total)
	})
}

// TestRuntimeMembership covers RegisterRuntime, UnregisterRuntime and ListRuntimes
func (s Suite) TestRuntimeMembership(t *testing.T) {
	// Runtime membership is not part of the seeded data, so each subtest uses its own runtime IDs and only looks at those
	listRuntimes := func(t *testing.T, prefix string) []components.RuntimeInfo {
		t.Helper()
		res, err := s.p.ListRuntimes(t.Context())
		require.NoError(t, err)

		ids := make([]string, len(res))
		for i, r := range res {
			ids[i] = r.RuntimeID
		}
		assert.Truef(t, slices.IsSorted(ids), "runtimes are not ordered by runtime ID: %v", ids)

		out := make([]components.RuntimeInfo, 0, len(res))
		for _, r := range res {
			if strings.HasPrefix(r.RuntimeID, prefix) {
				out = append(out, r)
			}
		}
		return out
	}

	// register records a runtime and schedules its removal, so later tests are not affected
	register := func(t *testing.T, runtimeID string, address string, ttl time.Duration) error {
		t.Helper()
		err := s.p.RegisterRuntime(t.Context(), components.RegisterRuntimeReq{RuntimeID: runtimeID, Address: address, TTL: ttl})
		if err == nil {
			t.Cleanup(func() { _ = s.p.UnregisterRuntime(context.WithoutCancel(t.Context()), runtimeID, address) })
		}
		return err
	}

	err := s.p.Seed(t.Context(), Spec{})
	require.NoError(t, err)

	t.Run("lists live runtimes in runtime ID order", func(t *testing.T) {
		const prefix = "mgmt-rt-list-"
		registeredAt := s.p.Now()
		err := register(t, prefix+"b", "10.5.0.2:7000", time.Minute)
		require.NoError(t, err)
		err = register(t, prefix+"a", "10.5.0.1:7000", time.Minute)
		require.NoError(t, err)
		err = register(t, prefix+"c", "10.5.0.3:7000", 2*time.Minute)
		require.NoError(t, err)

		got := listRuntimes(t, prefix)
		require.Len(t, got, 3)
		assert.Equal(t, prefix+"a", got[0].RuntimeID)
		assert.Equal(t, "10.5.0.1:7000", got[0].Address)
		assert.Equal(t, prefix+"b", got[1].RuntimeID)
		assert.Equal(t, "10.5.0.2:7000", got[1].Address)
		assert.Equal(t, prefix+"c", got[2].RuntimeID)
		assert.Equal(t, "10.5.0.3:7000", got[2].Address)

		assertTimeNear(t, registeredAt, got[0].LastHeartbeat)
		assertTimeNear(t, registeredAt.Add(time.Minute), got[0].ExpiresAt)
		assertTimeNear(t, registeredAt.Add(2*time.Minute), got[2].ExpiresAt)
	})

	t.Run("a live runtime ID cannot be claimed from another address", func(t *testing.T) {
		const id = "mgmt-rt-conflict"
		registeredAt := s.p.Now()
		err := register(t, id, "10.5.1.1:7000", time.Minute)
		require.NoError(t, err)

		// The rejected claim must not renew the lease either
		err = s.p.AdvanceClock(10 * time.Second)
		require.NoError(t, err)
		err = register(t, id, "10.5.1.2:7000", time.Hour)
		require.ErrorIs(t, err, components.ErrRuntimeIDInUse)

		// The original holder is unaffected
		got := listRuntimes(t, id)
		require.Len(t, got, 1)
		assert.Equal(t, "10.5.1.1:7000", got[0].Address)
		assertTimeNear(t, registeredAt, got[0].LastHeartbeat)
		assertTimeNear(t, registeredAt.Add(time.Minute), got[0].ExpiresAt)
	})

	t.Run("registering again from the same address renews the lease", func(t *testing.T) {
		const id = "mgmt-rt-renew"
		err := register(t, id, "10.5.2.1:7000", time.Minute)
		require.NoError(t, err)

		// Renew before the lease runs out, then move past the original expiry
		err = s.p.AdvanceClock(40 * time.Second)
		require.NoError(t, err)
		renewedAt := s.p.Now()
		err = register(t, id, "10.5.2.1:7000", time.Minute)
		require.NoError(t, err)

		got := listRuntimes(t, id)
		require.Len(t, got, 1)
		assertTimeNear(t, renewedAt, got[0].LastHeartbeat)
		assertTimeNear(t, renewedAt.Add(time.Minute), got[0].ExpiresAt)

		err = s.p.AdvanceClock(40 * time.Second)
		require.NoError(t, err)
		assert.Len(t, listRuntimes(t, id), 1, "the renewed lease must outlive the original expiry")
	})

	t.Run("an expired runtime is not listed and its ID can be claimed", func(t *testing.T) {
		const id = "mgmt-rt-expired"
		err := register(t, id, "10.5.3.1:7000", 30*time.Second)
		require.NoError(t, err)

		err = s.p.AdvanceClock(31 * time.Second)
		require.NoError(t, err)
		assert.Empty(t, listRuntimes(t, id))

		// A replica restarting with a new address takes over the expired ID
		err = register(t, id, "10.5.3.2:7000", time.Minute)
		require.NoError(t, err)
		got := listRuntimes(t, id)
		require.Len(t, got, 1)
		assert.Equal(t, "10.5.3.2:7000", got[0].Address)
	})

	t.Run("unregister only removes the record held by the address", func(t *testing.T) {
		const id = "mgmt-rt-unregister"
		err := register(t, id, "10.5.4.1:7000", time.Minute)
		require.NoError(t, err)

		// A replica that lost the ID cannot remove the new holder's record
		err = s.p.UnregisterRuntime(t.Context(), id, "10.5.4.2:7000")
		require.NoError(t, err)
		assert.Len(t, listRuntimes(t, id), 1)

		err = s.p.UnregisterRuntime(t.Context(), id, "10.5.4.1:7000")
		require.NoError(t, err)
		assert.Empty(t, listRuntimes(t, id))

		// It is idempotent
		err = s.p.UnregisterRuntime(t.Context(), id, "10.5.4.1:7000")
		require.NoError(t, err)
		err = s.p.UnregisterRuntime(t.Context(), "mgmt-rt-never-registered", "10.5.4.1:7000")
		require.NoError(t, err)

		// Once removed, the ID is free for any address
		err = register(t, id, "10.5.4.2:7000", time.Minute)
		require.NoError(t, err)
	})

	t.Run("membership is not affected by a restore", func(t *testing.T) {
		const id = "mgmt-rt-restore"
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		err = register(t, id, "10.5.5.1:7000", time.Hour)
		require.NoError(t, err)

		// Restoring a backup wipes the persistent data, which does not include runtime membership
		var buf bytes.Buffer
		err = s.p.Backup(t.Context(), &buf)
		require.NoError(t, err)
		err = s.p.Restore(t.Context(), bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)

		assert.Len(t, listRuntimes(t, id), 1)
	})
}
