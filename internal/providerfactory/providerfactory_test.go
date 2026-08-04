package providerfactory

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/instrument"
	"github.com/italypaleale/francis/components/sqlite"
)

func TestNewWrapsProviderWithInstrumentation(t *testing.T) {
	// Record spans into an in-memory recorder for the duration of the test, restoring the previous provider after
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
	})

	// Build an in-memory SQLite provider through the factory, which owns its connection
	p, err := New(slog.New(slog.DiscardHandler), sqlite.SQLiteProviderOptions{
		ConnectionString: "file:providerfactorytest?mode=memory",
	}, components.NewProviderConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		closeErr := p.Close()
		assert.NoError(t, closeErr)
	})

	// The factory must return the decorated provider
	_, wrapped := instrument.UnwrapProvider(p)
	assert.True(t, wrapped, "provider returned by the factory should be instrumented")

	// Init runs the schema migrations, which go through the instrumented connection
	err = p.Init(t.Context())
	require.NoError(t, err)

	spanNames := make(map[string]bool)
	for _, s := range sr.Ended() {
		spanNames[s.Name()] = true
	}

	assert.True(t, spanNames["provider.Init"], "expected a provider.Init span from the decorator")
	assert.True(t, spanNames["sqlite.exec"] || spanNames["sqlite.query"], "expected statement-level spans from the instrumented connection")
}

func TestNewStillRejectsInvalidOptions(t *testing.T) {
	_, err := New(slog.New(slog.DiscardHandler), nil, components.NewProviderConfig())
	require.Error(t, err)

	_, err = New(slog.New(slog.DiscardHandler), "not-an-option", components.NewProviderConfig())
	require.Error(t, err)
}
