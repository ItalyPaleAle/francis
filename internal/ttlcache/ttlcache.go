// This code was adapted from https://github.com/dapr/kit/tree/v0.15.4/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package ttlcache

import (
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	kclock "k8s.io/utils/clock"
)

// Cache is an efficient cache with a TTL.
type Cache[V any] struct {
	m         *haxmap.Map[string, cacheEntry[V]]
	clock     kclock.WithTicker
	stopped   atomic.Bool
	runningCh chan struct{}
	stopCh    chan struct{}
	maxTTL    time.Duration
}

// CacheOptions are options for NewCache.
type CacheOptions struct {
	// Initial size for the cache.
	// This is optional, and if empty will be left to the underlying library to decide.
	InitialSize int32

	// Interval to perform garbage collection.
	// This is optional, and defaults to 150s (2.5 minutes).
	CleanupInterval time.Duration

	// Maximum TTL value, if greater than 0
	MaxTTL time.Duration

	// Internal clock property, used for testing
	clock kclock.WithTicker
}

// NewCache returns a new cache with a TTL.
func NewCache[V any](opts *CacheOptions) *Cache[V] {
	var m *haxmap.Map[string, cacheEntry[V]]

	if opts == nil {
		opts = &CacheOptions{}
	}

	if opts.InitialSize > 0 {
		m = haxmap.New[string, cacheEntry[V]](uintptr(opts.InitialSize))
	} else {
		m = haxmap.New[string, cacheEntry[V]]()
	}

	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = 150 * time.Second
	}

	if opts.clock == nil {
		opts.clock = kclock.RealClock{}
	}

	c := &Cache[V]{
		m:      m,
		clock:  opts.clock,
		maxTTL: opts.MaxTTL,
		stopCh: make(chan struct{}),
	}
	c.startBackgroundCleanup(opts.CleanupInterval)

	return c
}

// Get returns an item from the cache.
// Items that have expired are not returned.
func (c *Cache[V]) Get(key string) (v V, ok bool) {
	val, ok := c.m.Get(key)
	if !ok || !val.exp.After(c.clock.Now()) {
		return v, false
	}
	return val.val, true
}

// Set an item in the cache.
func (c *Cache[V]) Set(key string, val V, ttl time.Duration) {
	if ttl < time.Millisecond {
		panic("invalid TTL: must be 1ms or greater")
	}

	if c.maxTTL > 0 && ttl > c.maxTTL {
		ttl = c.maxTTL
	}

	exp := c.clock.Now().Add(ttl)
	c.m.Set(key, cacheEntry[V]{
		val: val,
		exp: exp,
	})
}

// Delete an item from the cache
func (c *Cache[V]) Delete(key string) {
	c.m.Del(key)
}

// Cleanup removes all expired entries from the cache.
func (c *Cache[V]) Cleanup() {
	now := c.clock.Now()

	// Look for all expired keys and then remove them in bulk
	// This is more efficient than removing keys one-by-one
	// However, this could lead to a race condition where keys that are updated after ForEach ends are deleted nevertheless.
	// This is considered acceptable in this case as this is just a cache.
	keys := make([]string, 0)
	c.m.ForEach(func(k string, v cacheEntry[V]) bool {
		if !v.exp.After(now) {
			keys = append(keys, k)
		}
		return true
	})

	c.m.Del(keys...)
}

// Reset removes all entries from the cache.
func (c *Cache[V]) Reset() {
	// Look for all keys and then remove them in bulk
	// This is more efficient than removing keys one-by-one
	// However, this could lead to a race condition where keys that are updated after ForEach ends are deleted nevertheless.
	// This is considered acceptable in this case as this is just a cache.
	keys := make([]string, 0, c.m.Len())
	c.m.ForEach(func(k string, v cacheEntry[V]) bool {
		keys = append(keys, k)
		return true
	})

	c.m.Del(keys...)
}

func (c *Cache[V]) startBackgroundCleanup(d time.Duration) {
	c.runningCh = make(chan struct{})
	go func() {
		defer close(c.runningCh)

		t := c.clock.NewTicker(d)
		defer t.Stop()
		for {
			select {
			case <-c.stopCh:
				// Stop the background goroutine
				return
			case <-t.C():
				c.Cleanup()
			}
		}
	}()
}

// Stop the cache, stopping the background garbage collection process.
func (c *Cache[V]) Stop() {
	if c.stopped.CompareAndSwap(false, true) {
		close(c.stopCh)
	}
	<-c.runningCh
}

// Each item in the cache is stored in a cacheEntry, which includes the value as well as its expiration time.
type cacheEntry[V any] struct {
	val V
	exp time.Time
}
