package actor

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"

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

	listStatesRes  StateList
	listStatesErr  error
	listStatesType string
	listStatesOpts *ListStatesOpts
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

func (f *fakeHost) ListStates(_ context.Context, actorType string, opts *ListStatesOpts) (StateList, error) {
	f.listStatesType = actorType
	f.listStatesOpts = opts
	return f.listStatesRes, f.listStatesErr
}

func TestClientCanTarget(t *testing.T) {
	builtInType := ref.BuiltInActorTypePrefix + "cronjob.test"

	// A regular client cannot target built-in actors, but can target ordinary ones
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

	_, listStatesErr := c.ListStates(ctx, nil)
	require.ErrorIs(t, listStatesErr, ErrActorTypeReserved)

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

// TestClientRejectsReservedMethod verifies the public client refuses to invoke a reserved framework lifecycle method (such as bootstrap), while the privileged built-in client is allowed to drive it
func TestClientRejectsReservedMethod(t *testing.T) {
	ctx := context.Background()

	// A regular client cannot invoke a reserved method, and the guard returns before the nil host is dereferenced
	pub := NewActorClient[any]("widget", "w1", &Service{})
	_, err := pub.Invoke(ctx, "other", "id", ref.MethodBootstrap, nil)
	require.ErrorIs(t, err, ErrMethodReserved)
	_, err = pub.Peek(ctx, "other", "id", ref.MethodBootstrap, nil)
	require.ErrorIs(t, err, ErrMethodReserved)

	// The privileged built-in client may drive the reserved lifecycle, so it reaches the host instead of being rejected
	priv := NewBuiltInActorClient[any](builtinkey.Key{}, "widget", SingletonActorID, NewService(&fakeHost{}))
	_, err = priv.Invoke(ctx, "other", "id", ref.MethodBootstrap, nil)
	require.NoError(t, err)
}

// encodeState is a test helper that returns the envelope a host would produce for a state value
func encodeState(t *testing.T, v any) Envelope {
	t.Helper()

	data, err := msgpack.Marshal(v)
	require.NoError(t, err)

	return types.MsgpackEnvelope(data)
}

// TestClientListStates verifies the client lists its own actor type, decodes each state into T, and forwards the options untouched
func TestClientListStates(t *testing.T) {
	host := &fakeHost{
		listStatesRes: StateList{
			States: []StateInfo{
				{ActorID: "w1", Data: encodeState(t, 11)},
				{ActorID: "w2", Data: encodeState(t, 22)},
				// An actor listed without data keeps the zero value of T
				{ActorID: "w3"},
			},
			HasMore: true,
		},
	}
	svc := NewService(host)
	c := NewActorClient[int]("widget", "w1", svc)

	opts := &ListStatesOpts{IncludeData: true, After: "w0", Limit: 3}
	res, err := c.ListStates(t.Context(), opts)
	require.NoError(t, err)

	// The listing is always scoped to the actor type the client is bound to, and the options reach the host unchanged
	assert.Equal(t, "widget", host.listStatesType)
	assert.Same(t, opts, host.listStatesOpts)

	assert.True(t, res.HasMore)
	require.Len(t, res.States, 3)
	assert.Equal(t, TypedStateInfo[int]{ActorID: "w1", Data: 11}, res.States[0])
	assert.Equal(t, TypedStateInfo[int]{ActorID: "w2", Data: 22}, res.States[1])
	assert.Equal(t, TypedStateInfo[int]{ActorID: "w3", Data: 0}, res.States[2])
}

// TestClientListStatesDoesNotUseStateCache verifies that listing neither populates nor is served from the client's own state cache
func TestClientListStatesDoesNotUseStateCache(t *testing.T) {
	host := &fakeHost{
		getStateValue: 7,
		listStatesRes: StateList{
			States: []StateInfo{{ActorID: "w1", Data: encodeState(t, 99)}},
		},
	}
	svc := NewService(host)
	c := NewActorClient[int]("widget", "w1", svc)

	_, err := c.ListStates(t.Context(), &ListStatesOpts{IncludeData: true})
	require.NoError(t, err)

	// The listing included this actor's own state, but GetState must still read through to the host rather than serve the listed value
	state, err := c.GetState(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 7, state)
	assert.EqualValues(t, 1, host.getStateCalls.Load())
}

// TestClientListStatesReadOnly verifies listing is allowed during a Peek invocation, since it does not mutate the actor
func TestClientListStatesReadOnly(t *testing.T) {
	host := &fakeHost{
		listStatesRes: StateList{
			States: []StateInfo{{ActorID: "w1", Data: encodeState(t, 5)}},
		},
	}
	c := NewActorClient[int]("widget", "w1", NewService(host))

	res, err := c.ListStates(types.WithReadOnly(t.Context()), nil)
	require.NoError(t, err)
	require.Len(t, res.States, 1)
	assert.Equal(t, 5, res.States[0].Data)
}

// TestClientListStatesErrors verifies host errors and undecodable state are both surfaced to the caller
func TestClientListStatesErrors(t *testing.T) {
	t.Run("host error", func(t *testing.T) {
		listErr := errors.New("provider is down")
		c := NewActorClient[int]("widget", "w1", NewService(&fakeHost{listStatesErr: listErr}))

		_, err := c.ListStates(t.Context(), nil)
		require.ErrorIs(t, err, listErr)
	})

	t.Run("undecodable state", func(t *testing.T) {
		// A state that cannot be decoded into T fails the whole listing, and the error names the actor it came from
		host := &fakeHost{
			listStatesRes: StateList{
				States: []StateInfo{{ActorID: "w2", Data: encodeState(t, "not-a-number")}},
			},
		}
		c := NewActorClient[int]("widget", "w1", NewService(host))

		_, err := c.ListStates(t.Context(), &ListStatesOpts{IncludeData: true})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "w2")
	})
}
