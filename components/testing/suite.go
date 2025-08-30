package comptesting

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/actors/components"
)

// Suite implements a test suite for actor provider components.
type Suite struct {
	p ActorProviderTesting
}

func NewSuite(p ActorProviderTesting) *Suite {
	return &Suite{p: p}
}

func (s *Suite) Run(t *testing.T) {
	t.Run("actor state", s.TestState)
}

func (s *Suite) TestState(t *testing.T) {
	expectCollection := func(t *testing.T, expected ActorStateSpecCollection) {
		t.Helper()
		rows, err := s.p.GetAllActorState(t.Context())
		require.NoError(t, err)
		assert.True(t, expected.Equal(rows), "unexpected actor state collection: got=%v expected=%v", rows, expected)
	}

	// Seed the database again to ensure there's no state in the database
	require.NoError(t, s.p.Seed(t.Context(), GetSpec()))

	t.Run("get returns ErrNoState if no state", func(t *testing.T) {
		_, err := s.p.GetState(t.Context(), components.ActorRef{ActorType: "TestType", ActorID: "actor-1"})
		require.Error(t, err)
		assert.ErrorIs(t, err, components.ErrNoState)
	})

	t.Run("delete returns ErrNoState if no state", func(t *testing.T) {
		err := s.p.DeleteState(t.Context(), components.ActorRef{ActorType: "TestType", ActorID: "actor-1"})
		require.Error(t, err)
		assert.ErrorIs(t, err, components.ErrNoState)
	})

	t.Run("set get overwrite delete", func(t *testing.T) {
		ctx := t.Context()
		ref := components.ActorRef{ActorType: "TestType", ActorID: "actor-1"}

		data1 := []byte("hello world")
		err := s.p.SetState(ctx, ref, data1, components.SetStateOpts{})
		require.NoError(t, err)

		got, err := s.p.GetState(ctx, ref)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(data1, got))
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref.ActorType, ActorID: ref.ActorID, Data: data1}})

		data2 := []byte("goodbye")
		err = s.p.SetState(ctx, ref, data2, components.SetStateOpts{})
		require.NoError(t, err)

		got, err = s.p.GetState(ctx, ref)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(data2, got))
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref.ActorType, ActorID: ref.ActorID, Data: data2}})

		err = s.p.SetState(ctx, ref, []byte{}, components.SetStateOpts{})
		require.NoError(t, err)

		got, err = s.p.GetState(ctx, ref)
		require.NoError(t, err)
		assert.Len(t, got, 0)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref.ActorType, ActorID: ref.ActorID, Data: []byte{}}})

		err = s.p.DeleteState(ctx, ref)
		require.NoError(t, err)

		_, err = s.p.GetState(ctx, ref)
		require.ErrorIs(t, err, components.ErrNoState)
		expectCollection(t, ActorStateSpecCollection{})

		err = s.p.DeleteState(ctx, ref)
		require.Error(t, err)
		assert.ErrorIs(t, err, components.ErrNoState)
	})

	t.Run("ttl expiration", func(t *testing.T) {
		ctx := t.Context()
		ref2 := components.ActorRef{ActorType: "TestType", ActorID: "actor-ttl-1"}
		data := []byte("with-ttl")

		err := s.p.SetState(ctx, ref2, data, components.SetStateOpts{TTL: time.Second})
		require.NoError(t, err)

		_, err = s.p.GetState(ctx, ref2)
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref2.ActorType, ActorID: ref2.ActorID, Data: data}})

		s.p.AdvanceClock(1200 * time.Millisecond)
		err = s.p.CleanupExpired()
		require.NoError(t, err)

		_, err = s.p.GetState(ctx, ref2)
		require.ErrorIs(t, err, components.ErrNoState)
		expectCollection(t, ActorStateSpecCollection{})
	})

	t.Run("ttl extension on overwrite", func(t *testing.T) {
		ctx := t.Context()
		ref3 := components.ActorRef{ActorType: "TestType", ActorID: "actor-ttl-extend"}
		data1 := []byte("first")
		data2 := []byte("second")

		err := s.p.SetState(ctx, ref3, data1, components.SetStateOpts{TTL: 2 * time.Second})
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref3.ActorType, ActorID: ref3.ActorID, Data: data1}})

		s.p.AdvanceClock(time.Second)
		err = s.p.SetState(ctx, ref3, data2, components.SetStateOpts{TTL: 2 * time.Second})
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref3.ActorType, ActorID: ref3.ActorID, Data: data2}})

		s.p.AdvanceClock(1200 * time.Millisecond)
		_, err = s.p.GetState(ctx, ref3)
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref3.ActorType, ActorID: ref3.ActorID, Data: data2}})

		s.p.AdvanceClock(1200 * time.Millisecond)
		_, err = s.p.GetState(ctx, ref3)
		require.ErrorIs(t, err, components.ErrNoState)

		// GC hasn't run yet
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref3.ActorType, ActorID: ref3.ActorID, Data: data2}})

		err = s.p.CleanupExpired()
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{})
	})
}
