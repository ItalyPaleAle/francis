package local

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
	"github.com/italypaleale/francis/internal/testutil"
)

func newStateTestHost(t *testing.T) (*Host, *components_mocks.MockActorProvider) {
	provider := components_mocks.NewMockActorProvider(t)
	host := &Host{
		actorProvider:          provider,
		log:                    slog.New(slog.DiscardHandler),
		clock:                  clocktesting.NewFakeClock(time.Now()),
		providerRequestTimeout: 30 * time.Second,
	}
	return host, provider
}

func TestHostListStates(t *testing.T) {
	t.Run("maps the provider response and decodes the state", func(t *testing.T) {
		host, provider := newStateTestHost(t)

		data, err := msgpack.Marshal(42)
		require.NoError(t, err)

		provider.
			On("ListStates",
				mock.MatchedBy(testutil.MatchContextInterface),
				components.ListStatesReq{ActorType: "T", IncludeData: true, After: "a0", Limit: 2},
			).
			Return(components.ListStatesRes{
				States: []components.ActorStateInfo{
					{ActorID: "a1", Data: data},
					// State stored as an empty value has nothing to decode, so it must not produce an envelope
					{ActorID: "a2", Data: []byte{}},
				},
				HasMore: true,
			}, nil).
			Once()

		res, err := host.ListStates(t.Context(), "T", &actor.ListStatesOpts{IncludeData: true, After: "a0", Limit: 2})
		require.NoError(t, err)
		assert.True(t, res.HasMore)
		require.Len(t, res.States, 2)

		assert.Equal(t, "a1", res.States[0].ActorID)
		require.NotNil(t, res.States[0].Data)
		var got int
		require.NoError(t, res.States[0].Data.Decode(&got))
		assert.Equal(t, 42, got)

		assert.Equal(t, "a2", res.States[1].ActorID)
		assert.Nil(t, res.States[1].Data)

		provider.AssertExpectations(t)
	})

	t.Run("nil options request the provider defaults", func(t *testing.T) {
		host, provider := newStateTestHost(t)

		provider.
			On("ListStates",
				mock.MatchedBy(testutil.MatchContextInterface),
				components.ListStatesReq{ActorType: "T"},
			).
			Return(components.ListStatesRes{}, nil).
			Once()

		res, err := host.ListStates(t.Context(), "T", nil)
		require.NoError(t, err)
		assert.Empty(t, res.States)
		assert.False(t, res.HasMore)

		provider.AssertExpectations(t)
	})

	t.Run("rejects an invalid actor type before reaching the provider", func(t *testing.T) {
		host, provider := newStateTestHost(t)

		_, err := host.ListStates(t.Context(), "invalid/type", nil)
		require.Error(t, err)

		provider.AssertNotCalled(t, "ListStates", mock.Anything, mock.Anything)
	})

	t.Run("surfaces provider errors", func(t *testing.T) {
		host, provider := newStateTestHost(t)

		listErr := errors.New("provider is down")
		provider.
			On("ListStates", mock.MatchedBy(testutil.MatchContextInterface), mock.Anything).
			Return(components.ListStatesRes{}, listErr).
			Once()

		_, err := host.ListStates(t.Context(), "T", nil)
		require.ErrorIs(t, err, listErr)
	})
}
