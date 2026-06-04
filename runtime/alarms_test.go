package runtime

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// connectTestHost registers a host with the provider and tracks it in the host manager, returning its connection
func connectTestHost(t *testing.T, rt *Runtime, prov *standalone.StandaloneMemory, address string, at protocol.ActorHostType) *hostConn {
	t.Helper()

	res, err := prov.RegisterHost(t.Context(), components.RegisterHostReq{
		Address:    address,
		ActorTypes: []components.ActorHostType{{ActorType: at.ActorType, IdleTimeout: time.Minute}},
	})
	require.NoError(t, err)

	c := &hostConn{hostID: res.HostID, sessionID: "s1", address: address, actorTypes: []protocol.ActorHostType{at}}
	rt.hosts.Register(c)
	return c
}

// leaseTestAlarm sets an alarm and leases it through the provider, returning the resulting lease
func leaseTestAlarm(t *testing.T, prov *standalone.StandaloneMemory, hostID string, aref ref.AlarmRef, props ref.AlarmProperties) *ref.AlarmLease {
	t.Helper()

	require.NoError(t, prov.SetAlarm(t.Context(), aref, components.SetAlarmReq{AlarmProperties: props}))

	leases, err := prov.FetchAndLeaseUpcomingAlarms(t.Context(), components.FetchAndLeaseUpcomingAlarmsReq{Hosts: []string{hostID}})
	require.NoError(t, err)
	require.Len(t, leases, 1)
	return leases[0]
}

func TestDispatchAlarmCompleted(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.1.0.1:1", protocol.ActorHostType{ActorType: "T", MaxAttempts: 3, InitialRetryDelayMs: 100})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{DueTime: time.Now().Add(-time.Second), Data: []byte("payload")})

	var gotReq protocol.ExecuteAlarmRequest
	rt.sendToHost = func(_ context.Context, conn *hostConn, env *protocol.Envelope) (*protocol.Envelope, error) {
		assert.Equal(t, c.hostID, conn.hostID)
		assert.Equal(t, protocol.KindExecuteAlarm, env.Kind)
		require.NoError(t, env.DecodePayload(&gotReq))
		return env.ReplyWith(protocol.KindExecuteAlarmResponse, protocol.ExecuteAlarmResponse{ExecutionTimeUnixMs: time.Now().UnixMilli()})
	}

	status, err := rt.dispatchAlarm(t.Context(), lease)
	require.NoError(t, err)
	assert.Equal(t, executeAlarmStatusCompleted, status)
	assert.Equal(t, "T", gotReq.ActorType)
	assert.Equal(t, "a1", gotReq.ActorID)
	assert.Equal(t, "wake", gotReq.Name)
	assert.Equal(t, []byte("payload"), gotReq.Data)
}

func TestDispatchAlarmRetryableThenFatal(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.1.0.2:1", protocol.ActorHostType{ActorType: "T", MaxAttempts: 1, InitialRetryDelayMs: 100})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)})

	rt.sendToHost = func(_ context.Context, _ *hostConn, env *protocol.Envelope) (*protocol.Envelope, error) {
		return env.ErrorReply(protocol.NewError(protocol.ErrCodeInvokeFailed, "boom")), nil
	}

	// First failures are retryable while attempts are below the max
	status, _ := rt.dispatchAlarm(t.Context(), lease)
	assert.Equal(t, executeAlarmStatusRetryable, status)

	// Once attempts reach the max, the same error becomes fatal
	lease.IncreaseAttempts(time.Now())
	status, _ = rt.dispatchAlarm(t.Context(), lease)
	assert.Equal(t, executeAlarmStatusFatal, status)
}

func TestDispatchAlarmFatalUnsupportedType(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.1.0.3:1", protocol.ActorHostType{ActorType: "T", MaxAttempts: 100, InitialRetryDelayMs: 100})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)})

	rt.sendToHost = func(_ context.Context, _ *hostConn, env *protocol.Envelope) (*protocol.Envelope, error) {
		return env.ErrorReply(protocol.NewError(protocol.ErrCodeActorTypeUnsupported, "no such type")), nil
	}

	// An unsupported actor type can never succeed, so it is fatal regardless of remaining attempts
	status, _ := rt.dispatchAlarm(t.Context(), lease)
	assert.Equal(t, executeAlarmStatusFatal, status)
}

func TestDispatchAlarmTransportErrorIsRetryable(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.1.0.4:1", protocol.ActorHostType{ActorType: "T", MaxAttempts: 3, InitialRetryDelayMs: 100})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)})

	rt.sendToHost = func(_ context.Context, _ *hostConn, _ *protocol.Envelope) (*protocol.Envelope, error) {
		return nil, errors.New("stream broke")
	}

	status, err := rt.dispatchAlarm(t.Context(), lease)
	require.Error(t, err)
	assert.Equal(t, executeAlarmStatusRetryable, status)
}

func TestDispatchAlarmAbandonedWhenHostNotConnected(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.1.0.5:1", protocol.ActorHostType{ActorType: "T", MaxAttempts: 3, InitialRetryDelayMs: 100})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)})

	// The host disconnects: it is removed from the host manager even though the provider still has the placement
	require.True(t, rt.hosts.Remove(c.hostID, c.sessionID))

	called := false
	rt.sendToHost = func(_ context.Context, _ *hostConn, _ *protocol.Envelope) (*protocol.Envelope, error) {
		called = true
		return nil, nil
	}

	status, err := rt.dispatchAlarm(t.Context(), lease)
	require.NoError(t, err)
	assert.Equal(t, executeAlarmStatusAbandoned, status)
	assert.False(t, called, "must not dispatch to a host that is no longer connected")
}

func TestDispatchAlarmAbandonedWhenActorNotActive(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.1.0.6:1", protocol.ActorHostType{ActorType: "T", MaxAttempts: 3, InitialRetryDelayMs: 100})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)})

	// The actor is no longer active, so an active-only lookup finds nothing
	require.NoError(t, prov.RemoveActor(t.Context(), ref.NewActorRef("T", "a1")))

	status, err := rt.dispatchAlarm(t.Context(), lease)
	require.NoError(t, err)
	assert.Equal(t, executeAlarmStatusAbandoned, status)
}

func TestCompleteAlarmDeletesOneShot(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.1.0.7:1", protocol.ActorHostType{ActorType: "T"})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)})
	lease.SetExecutionTime(time.Now())

	err := rt.completeAlarm(t.Context(), lease, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	_, err = prov.GetAlarm(t.Context(), aref)
	require.ErrorIs(t, err, components.ErrNoAlarm, "a one-shot alarm should be deleted after completion")
}

func TestCompleteAlarmReschedulesRepeating(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.1.0.8:1", protocol.ActorHostType{ActorType: "T"})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	execTime := time.Now()
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{DueTime: execTime.Add(-time.Second), Interval: "PT1H"})
	lease.SetExecutionTime(execTime)

	err := rt.completeAlarm(t.Context(), lease, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	got, err := prov.GetAlarm(t.Context(), aref)
	require.NoError(t, err, "a repeating alarm should still exist after completion")
	// The next due time is one hour after the execution time
	assert.WithinDuration(t, execTime.Add(time.Hour), got.DueTime, time.Second)
}

func TestDrainActiveAlarmsWaitsForInflight(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.1.0.10:1", protocol.ActorHostType{ActorType: "T", MaxAttempts: 3, InitialRetryDelayMs: 100})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)})

	started := make(chan struct{})
	release := make(chan struct{})
	rt.sendToHost = func(_ context.Context, _ *hostConn, env *protocol.Envelope) (*protocol.Envelope, error) {
		close(started)
		<-release
		return env.ReplyWith(protocol.KindExecuteAlarmResponse, protocol.ExecuteAlarmResponse{})
	}

	// Start an in-flight execution (the alarm is already due, so it runs immediately)
	require.NoError(t, rt.enqueueAlarms([]*ref.AlarmLease{lease}))
	<-started

	drained := make(chan struct{})
	go func() {
		rt.drainActiveAlarms()
		close(drained)
	}()

	// The drain must not complete while the alarm is still executing
	select {
	case <-drained:
		t.Fatal("drain returned before the in-flight alarm finished")
	case <-time.After(100 * time.Millisecond):
	}

	// Once the alarm finishes, the drain completes
	close(release)
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not return after the in-flight alarm finished")
	}
}

func TestDrainActiveAlarmsTimesOut(t *testing.T) {
	rt, prov := newTestRuntime(t, WithShutdownGracePeriod(150*time.Millisecond))
	c := connectTestHost(t, rt, prov, "10.1.0.11:1", protocol.ActorHostType{ActorType: "T", MaxAttempts: 3, InitialRetryDelayMs: 100})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)})

	started := make(chan struct{})
	release := make(chan struct{})
	// Release the alarm at the end so its goroutine does not linger past the test
	defer close(release)
	rt.sendToHost = func(_ context.Context, _ *hostConn, env *protocol.Envelope) (*protocol.Envelope, error) {
		close(started)
		<-release
		return env.ReplyWith(protocol.KindExecuteAlarmResponse, protocol.ExecuteAlarmResponse{})
	}

	require.NoError(t, rt.enqueueAlarms([]*ref.AlarmLease{lease}))
	<-started

	// The alarm never finishes within the grace period, so the drain returns after the deadline
	start := time.Now()
	rt.drainActiveAlarms()
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 150*time.Millisecond)
	assert.Less(t, elapsed, 2*time.Second)
}

func TestDrainActiveAlarmsStopsNewExecutions(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.1.0.12:1", protocol.ActorHostType{ActorType: "T", MaxAttempts: 3, InitialRetryDelayMs: 100})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)})

	called := false
	rt.sendToHost = func(_ context.Context, _ *hostConn, _ *protocol.Envelope) (*protocol.Envelope, error) {
		called = true
		return nil, nil
	}

	// Nothing is in flight, so the drain returns immediately and flips the runtime into draining mode
	rt.drainActiveAlarms()

	// After draining, enqueueing a due alarm must not start a new execution
	require.NoError(t, rt.enqueueAlarms([]*ref.AlarmLease{lease}))
	time.Sleep(50 * time.Millisecond)
	assert.False(t, called, "no new alarm should be dispatched once the runtime is draining")
}

func TestFetchAndEnqueueAlarmsScopedToConnectedHosts(t *testing.T) {
	rt, prov := newTestRuntime(t)

	// With no connected hosts there is nothing to fetch
	require.NoError(t, rt.fetchAndEnqueueAlarms(t.Context()))

	_ = connectTestHost(t, rt, prov, "10.1.0.9:1", protocol.ActorHostType{ActorType: "T", MaxAttempts: 3, InitialRetryDelayMs: 100})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	require.NoError(t, prov.SetAlarm(t.Context(), aref, components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)},
	}))

	// The due alarm is dispatched to the connected host
	dispatched := make(chan protocol.ExecuteAlarmRequest, 1)
	rt.sendToHost = func(_ context.Context, _ *hostConn, env *protocol.Envelope) (*protocol.Envelope, error) {
		var req protocol.ExecuteAlarmRequest
		_ = env.DecodePayload(&req)
		dispatched <- req
		return env.ReplyWith(protocol.KindExecuteAlarmResponse, protocol.ExecuteAlarmResponse{})
	}

	require.NoError(t, rt.fetchAndEnqueueAlarms(t.Context()))

	select {
	case req := <-dispatched:
		assert.Equal(t, "a1", req.ActorID)
	case <-time.After(2 * time.Second):
		t.Fatal("expected the due alarm to be dispatched to the connected host")
	}
}

func TestExecuteAlarmDispatchesOffTheProcessorLoop(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.1.0.20:1", protocol.ActorHostType{
		ActorType:           "T",
		MaxAttempts:         3,
		InitialRetryDelayMs: 100,
	})

	// Lease two due alarms for the same host
	err := prov.SetAlarm(t.Context(), ref.NewAlarmRef("T", "a1", "wake"), components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)},
	})
	require.NoError(t, err)

	err = prov.SetAlarm(t.Context(), ref.NewAlarmRef("T", "a2", "wake"), components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{DueTime: time.Now().Add(-time.Second)},
	})
	require.NoError(t, err)

	leases, err := prov.FetchAndLeaseUpcomingAlarms(t.Context(), components.FetchAndLeaseUpcomingAlarmsReq{
		Hosts: []string{c.hostID},
	})
	require.NoError(t, err)
	require.Len(t, leases, 2)

	// Every dispatch blocks until released, simulating a hung host
	started := make(chan string, 2)
	release := make(chan struct{})
	defer close(release)
	rt.sendToHost = func(_ context.Context, _ *hostConn, env *protocol.Envelope) (*protocol.Envelope, error) {
		var req protocol.ExecuteAlarmRequest
		_ = env.DecodePayload(&req)
		started <- req.ActorID
		<-release
		return env.ReplyWith(protocol.KindExecuteAlarmResponse, protocol.ExecuteAlarmResponse{})
	}

	// Invoke the processor callback serially, exactly as the event queue does
	// If executeAlarm ran the dispatch inline, the first hung call would never return and the second would never start
	for _, lease := range leases {
		rt.executeAlarm(lease)
	}

	// Both dispatches must be in flight even though the first is still hung
	got := map[string]bool{}
	for range leases {
		select {
		case id := <-started:
			got[id] = true
		case <-time.After(2 * time.Second):
			t.Fatal("a dispatch did not start while another was hung")
		}
	}
	assert.Truef(t, got["a1"] && got["a2"], "both alarms should have been dispatched concurrently; a1:%v a2:%v", got["a1"], got["a2"])
}

func TestDispatchAlarmHostTimeoutRetryable(t *testing.T) {
	rt, prov := newTestRuntime(t, WithAlarmExecutionTimeout(50*time.Millisecond))
	c := connectTestHost(t, rt, prov, "10.1.0.21:1", protocol.ActorHostType{
		ActorType:           "T",
		MaxAttempts:         3,
		InitialRetryDelayMs: 100,
	})

	aref := ref.NewAlarmRef("T", "a1", "wake")
	lease := leaseTestAlarm(t, prov, c.hostID, aref, ref.AlarmProperties{
		DueTime: time.Now().Add(-time.Second),
	})

	// The host accepts the request but never replies, so the round-trip must time out via the bounded context
	rt.sendToHost = func(ctx context.Context, _ *hostConn, _ *protocol.Envelope) (*protocol.Envelope, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	start := time.Now()
	status, err := rt.dispatchAlarm(t.Context(), lease)
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.Equal(t, executeAlarmStatusRetryable, status)
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
	assert.Less(t, elapsed, 2*time.Second)
}
