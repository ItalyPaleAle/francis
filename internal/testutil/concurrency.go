package testutil

import (
	"sync/atomic"
	"testing"
	"time"
)

func WaitForGoroutines(t *testing.T, want int32, counter *atomic.Int32) {
	t.Helper()

	var i int
	for counter.Load() != want {
		if i == 1000 {
			t.Fatalf("Waited too long for goroutines to start")
		}
		i++

		time.Sleep(500 * time.Microsecond)
	}
}
