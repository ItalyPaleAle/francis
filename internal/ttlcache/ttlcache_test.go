// This code was adapted from https://github.com/dapr/kit/tree/v0.15.4/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package ttlcache

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"
)

func TestCache(t *testing.T) {
	clock := &clocktesting.FakeClock{}
	clock.SetTime(time.Now())

	cache := NewCache[string](&CacheOptions{
		InitialSize:     10,
		CleanupInterval: 20 * time.Second,
		MaxTTL:          15 * time.Second,
		clock:           clock,
	})
	defer cache.Stop()

	// Set values in the cache
	cache.Set("key1", "val1", 2*time.Second)
	cache.Set("key2", "val2", 5*time.Second)
	cache.Set("key3", "val3", 30*time.Second) // Max TTL is 15s
	cache.Set("key4", "val4", 5*time.Second)

	// Retrieve values
	for i := range 16 {
		v, ok := cache.Get("key1")
		if i < 2 {
			require.True(t, ok)
			require.Equal(t, "val1", v)
		} else {
			require.False(t, ok)
		}

		v, ok = cache.Get("key2")
		if i < 5 {
			require.True(t, ok)
			require.Equal(t, "val2", v)
		} else {
			require.False(t, ok)
		}

		v, ok = cache.Get("key3")
		if i < 15 {
			require.True(t, ok)
			require.Equal(t, "val3", v)
		} else {
			require.False(t, ok)
		}

		v, ok = cache.Get("key4")
		if i < 1 {
			require.True(t, ok)
			require.Equal(t, "val4", v)

			// Delete from the cache
			cache.Delete("key4")
		} else {
			require.False(t, ok)
		}

		// Advance the clock
		clock.Step(time.Second)
		runtime.Gosched()
		time.Sleep(20 * time.Millisecond)
	}

	// Values should still be in the cache as they haven't been cleaned up yet
	require.EqualValues(t, 3, cache.m.Len())

	// Advance the clock a bit more to make sure the cleanup runs
	clock.Step(5 * time.Second)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		if !assert.EqualValues(c, 0, cache.m.Len()) {
			runtime.Gosched()
		}
	}, time.Second, 20*time.Millisecond)
}

func TestCacheCleanupRemovesExpiredEntriesAtBoundary(t *testing.T) {
	clock := &clocktesting.FakeClock{}
	clock.SetTime(time.Now())

	cache := NewCache[string](&CacheOptions{
		CleanupInterval: time.Hour,
		clock:           clock,
	})
	defer cache.Stop()

	cache.Set("key", "value", time.Second)

	clock.Step(time.Second)
	cache.Cleanup()

	_, ok := cache.Get("key")
	require.False(t, ok)

	assert.EqualValues(t, 0, cache.m.Len())
}
