package runtime

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
)

func TestHostManagerRegisterAndGet(t *testing.T) {
	m := NewHostManager(nil)

	c := &hostConn{hostID: "h1", sessionID: "s1", address: "1.2.3.4:443"}
	superseded := m.Register(c)
	assert.Nil(t, superseded)

	got, ok := m.Get("h1")
	require.True(t, ok)
	assert.Same(t, c, got)
	assert.Equal(t, 1, m.Count())
}

func TestHostManagerRegisterSupersedesOldSession(t *testing.T) {
	m := NewHostManager(nil)

	old := &hostConn{hostID: "h1", sessionID: "s1"}
	require.Nil(t, m.Register(old))

	// A new session for the same host supersedes the old one
	fresh := &hostConn{hostID: "h1", sessionID: "s2"}
	superseded := m.Register(fresh)
	require.NotNil(t, superseded)
	assert.Same(t, old, superseded)

	// The newest session wins
	got, ok := m.Get("h1")
	require.True(t, ok)
	assert.Same(t, fresh, got)
	assert.Equal(t, 1, m.Count())
}

func TestHostManagerRegisterSameSessionDoesNotSupersede(t *testing.T) {
	m := NewHostManager(nil)

	c1 := &hostConn{hostID: "h1", sessionID: "s1"}
	require.Nil(t, m.Register(c1))

	// Re-registering with the same session ID must not report a superseded session
	c2 := &hostConn{hostID: "h1", sessionID: "s1"}
	assert.Nil(t, m.Register(c2))
}

func TestHostManagerRemoveOnlyMatchingSession(t *testing.T) {
	m := NewHostManager(nil)

	fresh := &hostConn{hostID: "h1", sessionID: "s2"}
	m.Register(fresh)

	// A stale session teardown must not evict the newer session
	removed := m.Remove("h1", "s1")
	assert.False(t, removed)
	_, ok := m.Get("h1")
	assert.True(t, ok)

	// The current session can be removed
	removed = m.Remove("h1", "s2")
	assert.True(t, removed)
	_, ok = m.Get("h1")
	assert.False(t, ok)
	assert.Equal(t, 0, m.Count())
}

func TestHostManagerRemoveUnknownHost(t *testing.T) {
	m := NewHostManager(nil)
	assert.False(t, m.Remove("nope", "s1"))
}

func TestHostManagerConnectedHostIDsExcludesDraining(t *testing.T) {
	m := NewHostManager(nil)

	active := &hostConn{hostID: "h1", sessionID: "s1"}
	draining := &hostConn{hostID: "h2", sessionID: "s2"}
	draining.setDraining()

	m.Register(active)
	m.Register(draining)

	ids := m.ConnectedHostIDs()
	assert.Len(t, ids, 1)
	assert.True(t, slices.Contains(ids, "h1"))
	assert.False(t, slices.Contains(ids, "h2"))

	// Both are still counted as connected
	assert.Equal(t, 2, m.Count())
}

func TestHostManagerListHosts(t *testing.T) {
	t.Run("returns hosts from the provider", func(t *testing.T) {
		now := time.Now()
		expected := []components.HostInfo{
			{HostID: "h1", Address: "1.2.3.4:443", LastHealthCheck: now.Add(-2 * time.Second)},
			{HostID: "h2", Address: "5.6.7.8:443", LastHealthCheck: now.Add(-5 * time.Second)},
		}

		provider := components_mocks.NewMockActorProvider(t)
		provider.EXPECT().
			ListHosts(t.Context()).
			Return(expected, nil).
			Once()

		m := NewHostManager(provider)
		hosts, err := m.ListHosts(t.Context())
		require.NoError(t, err)
		assert.Equal(t, expected, hosts)
	})

	t.Run("propagates the provider error", func(t *testing.T) {
		sentinel := errors.New("provider failed")

		provider := components_mocks.NewMockActorProvider(t)
		provider.EXPECT().
			ListHosts(t.Context()).
			Return(nil, sentinel).
			Once()

		m := NewHostManager(provider)
		hosts, err := m.ListHosts(t.Context())
		require.ErrorIs(t, err, sentinel)
		assert.Nil(t, hosts)
	})
}
