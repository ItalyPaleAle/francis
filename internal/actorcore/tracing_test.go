package actorcore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkTrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/italypaleale/francis/internal/ref"
)

func TestInvokeProducesSpans(t *testing.T) {
	// Record spans into an in-memory recorder for the duration of the test, restoring the previous provider after
	sr := tracetest.NewSpanRecorder()
	tp := sdkTrace.NewTracerProvider(sdkTrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
	})

	// Drive a cold-start local invocation through the shared manager
	m := newMessagingManager(t, echoFactory)
	resolver := &fakeResolver{
		localHostID: "h1",
		placements:  []*Placement{{HostID: "h1", Address: "addr1"}},
	}
	peer := &fakePeer{}
	env, err := m.Invoke(t.Context(), resolver, peer, ref.NewActorRef("testactor", "a1"), "ping", "x", false)
	require.NoError(t, err)
	assert.Equal(t, "echo:ping:x", decodeEnvelope(t, env))

	// Index the recorded spans by name
	byName := make(map[string]sdkTrace.ReadOnlySpan)
	for _, s := range sr.Ended() {
		byName[s.Name()] = s
	}

	// The invocation, the actor execution, and the cold-start activation must all have been spanned
	invoke, ok := byName["actor.invoke"]
	require.True(t, ok, "expected an actor.invoke span")
	execute, ok := byName["actor.execute"]
	require.True(t, ok, "expected an actor.execute span")
	activate, ok := byName["actor.activate"]
	require.True(t, ok, "expected an actor.activate span")

	// actor.invoke is the root, so it carries no valid parent
	assert.False(t, invoke.Parent().IsValid(), "actor.invoke should be the root span")

	// actor.execute and actor.activate run under the invocation, sharing its trace and pointing at it as their parent
	assert.Equal(t, invoke.SpanContext().TraceID(), execute.SpanContext().TraceID())
	assert.Equal(t, invoke.SpanContext().SpanID(), execute.Parent().SpanID())
	assert.Equal(t, invoke.SpanContext().SpanID(), activate.Parent().SpanID())

	// The invocation span is tagged with the actor target it resolved
	assertHasAttribute(t, invoke, "francis.actor.type", "testactor")
	assertHasAttribute(t, invoke, "francis.actor.id", "a1")
	assertHasAttribute(t, invoke, "francis.actor.method", "ping")
}

// assertHasAttribute asserts a span carries a string attribute with the expected value
func assertHasAttribute(t *testing.T, span sdkTrace.ReadOnlySpan, key string, want string) {
	t.Helper()

	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			// Found it
			assert.Equal(t, want, attr.Value.AsString())
			return
		}
	}

	t.Errorf("span %q is missing attribute %q", span.Name(), key)
}
