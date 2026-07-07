package actor

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/builtinkey"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/types"
)

// fakeHost is a minimal actor.Host used to test Client's state caching and read-only guards without pulling in a real host implementation
type fakeHost struct {
	getStateCalls atomic.Int32
	getStateDelay time.Duration
	getStateValue int
	getStateErr   error
}

func (f *fakeHost) Invoke(context.Context, string, string, string, any, ...InvokeOption) (Envelope, error) {
	return nil, nil
}

func (f *fakeHost) InvokeStream(context.Context, string, string, string, string, io.Reader, ...InvokeOption) (string, io.ReadCloser, error) {
	return "", nil, nil
}

func (f *fakeHost) Peek(context.Context, string, string, string, any, ...InvokeOption) (Envelope, error) {
	return nil, nil
}

func (f *fakeHost) PeekStream(context.Context, string, string, string, string, io.Reader, ...InvokeOption) (string, io.ReadCloser, error) {
	return "", nil, nil
}

func (f *fakeHost) HaltAll() error {
	return nil
}

func (f *fakeHost) Halt(string, string) error {
	return nil
}

func (f *fakeHost) HaltDeferred(string, string) {
	// Nop
}

func (f *fakeHost) GetAlarm(context.Context, string, string, string) (AlarmProperties, error) {
	return AlarmProperties{}, nil
}

func (f *fakeHost) SetAlarm(context.Context, string, string, string, AlarmProperties) error {
	return nil
}

func (f *fakeHost) DeleteAlarm(context.Context, string, string, string) error {
	return nil
}

func (f *fakeHost) Dispatch(context.Context, string, string, string, any, JobProperties) (string, error) {
	return "", nil
}

func (f *fakeHost) GetJob(context.Context, string) (JobInfo, error) {
	return JobInfo{}, nil
}

func (f *fakeHost) ListJobs(context.Context, string, string) ([]JobInfo, error) {
	return nil, nil
}

func (f *fakeHost) CancelJob(context.Context, string, string, string) error {
	return nil
}

func (f *fakeHost) RetryJob(context.Context, string) (string, error) {
	return "", nil
}

func (f *fakeHost) SetState(context.Context, string, string, any, *SetStateOpts) error { return nil }

func (f *fakeHost) GetState(_ context.Context, _ string, _ string, dest any) error {
	f.getStateCalls.Add(1)
	if f.getStateDelay > 0 {
		time.Sleep(f.getStateDelay)
	}
	if f.getStateErr != nil {
		return f.getStateErr
	}

	p, ok := dest.(*int)
	if ok {
		*p = f.getStateValue
	}
	return nil
}

func (f *fakeHost) DeleteState(context.Context, string, string) error { return nil }

func TestClientCanTarget(t *testing.T) {
	builtInType := ref.BuiltInActorTypePrefix + "cronjob.test"

	// A normal client cannot target built-in actors, but can target ordinary ones
	pub := &client[any]{}
	assert.False(t, pub.canTarget(builtInType))
	assert.True(t, pub.canTarget("ordinary"))

	// The privileged built-in client can target anything
	priv := &client[any]{privileged: true}
	assert.True(t, priv.canTarget(builtInType))
	assert.True(t, priv.canTarget("ordinary"))
}

// TestClientRejectsBuiltInTarget verifies the public client refuses to operate on a built-in actor, returning before it reaches the host
func TestClientRejectsBuiltInTarget(t *testing.T) {
	ctx := context.Background()
	builtInType := ref.BuiltInActorTypePrefix + "cronjob.test"

	// The guard returns before the nil host is dereferenced, so a zero-value Service is enough
	c := NewActorClient[map[string]any](builtInType, "singleton", &Service{})

	err := c.SetState(ctx, nil, nil)
	require.ErrorIs(t, err, ErrActorTypeReserved)

	_, getErr := c.GetState(ctx)
	require.ErrorIs(t, getErr, ErrActorTypeReserved)

	err = c.DeleteState(ctx)
	require.ErrorIs(t, err, ErrActorTypeReserved)
	err = c.SetAlarm(ctx, "a", AlarmProperties{})
	require.ErrorIs(t, err, ErrActorTypeReserved)
	err = c.DeleteAlarm(ctx, "a")
	require.ErrorIs(t, err, ErrActorTypeReserved)

	_, dispatchErr := c.Dispatch(ctx, "run", nil)
	require.ErrorIs(t, dispatchErr, ErrActorTypeReserved)

	_, listErr := c.ListJobs(ctx)
	require.ErrorIs(t, listErr, ErrActorTypeReserved)

	err = c.CancelJob(ctx, "job")
	require.ErrorIs(t, err, ErrActorTypeReserved)

	// Invoking a built-in target is rejected regardless of which actor the client is bound to
	_, err = c.Invoke(ctx, builtInType, "singleton", "run", nil)
	require.ErrorIs(t, err, ErrActorTypeReserved)

	// Peeking a built-in target is rejected the same way
	_, err = c.Peek(ctx, builtInType, "singleton", "run", nil)
	require.ErrorIs(t, err, ErrActorTypeReserved)
}

// TestClientReadOnlyGuards verifies that every state-mutating client call is rejected under a read-only (Peek) context, while GetState and Peek/Invoke of other actors remain unaffected
func TestClientReadOnlyGuards(t *testing.T) {
	host := &fakeHost{getStateValue: 7}
	svc := NewService(host)
	c := NewActorClient[int]("widget", "w1", svc)

	ctx := types.WithReadOnly(t.Context())

	err := c.SetState(ctx, 1, nil)
	require.ErrorIs(t, err, ErrReadOnly)

	err = c.DeleteState(ctx)
	require.ErrorIs(t, err, ErrReadOnly)

	err = c.SetAlarm(ctx, "a", AlarmProperties{})
	require.ErrorIs(t, err, ErrReadOnly)

	err = c.DeleteAlarm(ctx, "a")
	require.ErrorIs(t, err, ErrReadOnly)

	_, err = c.Dispatch(ctx, "run", nil)
	require.ErrorIs(t, err, ErrReadOnly)

	// GetState is always allowed, even under a read-only context
	state, err := c.GetState(ctx)
	require.NoError(t, err)
	assert.Equal(t, 7, state)

	// Invoking or peeking another actor is unaffected by this actor's own read-only marker
	_, err = c.Invoke(ctx, "other", "id", "method", nil)
	require.NoError(t, err)
	_, err = c.Peek(ctx, "other", "id", "method", nil)
	require.NoError(t, err)
}

// TestClientGetStateConcurrentMissCollapsesToSingleFetch verifies that concurrent GetState calls racing on an empty cache (as happens with overlapping Peek turns) result in exactly one provider fetch
func TestClientGetStateConcurrentMissCollapsesToSingleFetch(t *testing.T) {
	host := &fakeHost{getStateValue: 42, getStateDelay: 50 * time.Millisecond}
	svc := NewService(host)
	c := NewActorClient[int]("widget", "w1", svc)

	const numCallers = 20
	results := make([]int, numCallers)
	errs := make([]error, numCallers)

	var wg sync.WaitGroup
	for i := range numCallers {
		wg.Go(func() {
			results[i], errs[i] = c.GetState(t.Context())
		})
	}
	wg.Wait()

	for i := range numCallers {
		require.NoError(t, errs[i])
		assert.Equal(t, 42, results[i])
	}

	assert.EqualValues(t, 1, host.getStateCalls.Load(), "concurrent cache misses must collapse into a single provider fetch")
}

// TestClientPeek verifies Peek forwards to the service and honors the same built-in-type guard as Invoke
func TestClientPeek(t *testing.T) {
	host := &fakeHost{}
	svc := NewService(host)
	c := NewActorClient[int]("widget", "w1", svc)

	_, err := c.Peek(t.Context(), "other", "id", "method", nil)
	require.NoError(t, err)

	builtInType := ref.BuiltInActorTypePrefix + "cronjob.test"
	_, err = c.Peek(t.Context(), builtInType, "singleton", "run", nil)
	require.ErrorIs(t, err, ErrActorTypeReserved)
}

// TestNewBuiltInActorClientIsPrivileged verifies the framework's client skips the built-in guard
func TestNewBuiltInActorClientIsPrivileged(t *testing.T) {
	builtInType := ref.BuiltInActorTypePrefix + "cronjob.test"

	c := NewBuiltInActorClient[map[string]any](builtinkey.Key{}, builtInType, "singleton", &Service{})
	cc, ok := c.(*client[map[string]any])
	require.True(t, ok)
	assert.True(t, cc.privileged)
	assert.True(t, cc.canTarget(builtInType))
}
