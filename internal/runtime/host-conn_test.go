package runtime

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/italypaleale/francis/protocol"
)

func TestHostConnActorTypesConcurrent(t *testing.T) {
	c := &hostConn{hostID: "h1", sessionID: "s1"}
	c.setActorTypes([]protocol.ActorHostType{{ActorType: "T", MaxAttempts: 1}})

	const iterations = 2000
	wg := sync.WaitGroup{}

	// Writers replace the whole slice, mirroring repeated health checks carrying actor types
	for w := range 4 {
		wg.Go(func() {
			for range iterations {
				c.setActorTypes([]protocol.ActorHostType{
					{ActorType: "T", MaxAttempts: w + 1},
				})
			}
		})
	}

	// Readers range over the current snapshot concurrently with the writers
	for range 4 {
		wg.Go(func() {
			for range iterations {
				_, _ = c.actorTypeConfig("T")
			}
		})
	}

	wg.Wait()

	// The actor type is still resolvable after the concurrent churn
	at, ok := c.actorTypeConfig("T")
	assert.True(t, ok)
	assert.Equal(t, "T", at.ActorType)
}

// TestHostConnActorTypeConfigUnset returns not-found when no actor types have been advertised yet
func TestHostConnActorTypeConfigUnset(t *testing.T) {
	c := &hostConn{hostID: "h1", sessionID: "s1"}

	_, ok := c.actorTypeConfig("T")
	assert.False(t, ok)
}
