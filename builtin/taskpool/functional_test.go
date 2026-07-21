package taskpool_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/builtin/taskpool"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/francis/internal/testutil"
)

// testRuntimePSK is the shared runtime PSK the functional tests derive their cluster CA from
var testRuntimePSK = []byte("taskpool-test-runtime-psk-0123456789")

// startHost builds and runs a local host with the given pool registered, waiting until it is ready and cleaning it up when the test ends
func startHost(t *testing.T, dbPath string, pool *taskpool.TaskPool) *local.Host {
	t.Helper()

	host, err := local.NewHost(
		local.WithAddress(testutil.FreeUDPAddr(t)),
		local.WithSQLiteProvider(sqlite.SQLiteProviderOptions{ConnectionString: dbPath}),
		local.WithRuntimePSKs(testRuntimePSK),
		// Poll frequently so submitted tasks start without waiting a full default interval
		local.WithAlarmsPollInterval(200*time.Millisecond),
		local.WithLogger(slog.New(slog.DiscardHandler)),
	)
	require.NoError(t, err)

	err = host.RegisterBuiltInActor(pool)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		errCh <- host.Run(t.Context())
	}()

	select {
	case <-host.Ready():
	case <-time.After(15 * time.Second):
		t.Fatal("host did not register")
	}

	t.Cleanup(func() {
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
			t.Error("host did not shut down")
		}
	})

	return host
}

// TestStrictConcurrencyPerHost verifies a host never runs more tasks at once than its configured concurrency, while still running them in parallel
func TestStrictConcurrencyPerHost(t *testing.T) {
	const (
		concurrency = 2
		numTasks    = 6
	)

	var (
		running   atomic.Int32
		maxSeen   atomic.Int32
		completed sync.WaitGroup
	)
	completed.Add(numTasks)

	// The handler tracks how many run at once, holding the slot briefly so tasks overlap
	handler := func(ctx context.Context, task taskpool.Task) error {
		defer completed.Done()

		now := running.Add(1)
		for {
			prev := maxSeen.Load()
			if now <= prev || maxSeen.CompareAndSwap(prev, now) {
				break
			}
		}

		time.Sleep(500 * time.Millisecond)
		running.Add(-1)
		return nil
	}

	pool, err := taskpool.New("concurrency", taskpool.WithHandler(handler), taskpool.WithConcurrency(concurrency))
	require.NoError(t, err)

	host := startHost(t, filepath.Join(t.TempDir(), "strict.db"), pool)
	svc := pool.Service(host.Service())

	// Submit all the tasks
	for range numTasks {
		_, err = svc.Submit(t.Context(), nil)
		require.NoError(t, err)
	}

	// Wait for every task to finish
	done := make(chan struct{})
	go func() {
		completed.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("tasks did not all complete")
	}

	// The strict guarantee: never more than the configured concurrency ran at once
	assert.LessOrEqual(t, int(maxSeen.Load()), concurrency, "host exceeded its strict concurrency")
	// Sanity: parallelism actually happened, so the limit is a real bound and not just serialization
	assert.Equal(t, int32(concurrency), maxSeen.Load(), "expected the host to run tasks in parallel up to the limit")
}

// TestRejectReroute verifies a task a host declines is re-routed and run on another host, without failing
func TestRejectReroute(t *testing.T) {
	const numTasks = 4

	// Both hosts share one database so they see the same placement and queue
	dbPath := filepath.Join(t.TempDir(), "reroute.db")

	// Host A declines every task, so anything placed on it must be re-routed
	poolA, err := taskpool.New("reroute",
		taskpool.WithConcurrency(numTasks),
		taskpool.WithAccept(func(context.Context, taskpool.Task) bool { return false }),
		taskpool.WithHandler(func(context.Context, taskpool.Task) error {
			return assertUnreachable(t)
		}),
	)
	require.NoError(t, err)

	// Host B accepts and runs every task
	// Tasks are delivered at least once, so a task's handler can run more than once; dedupe by task ID so re-deliveries do not over-count
	var (
		mu       sync.Mutex
		seen     = make(map[string]struct{}, numTasks)
		done     = make(chan struct{})
		doneOnce sync.Once
	)
	poolB, err := taskpool.New("reroute",
		taskpool.WithConcurrency(numTasks),
		taskpool.WithAccept(func(context.Context, taskpool.Task) bool { return true }),
		taskpool.WithHandler(func(_ context.Context, task taskpool.Task) error {
			mu.Lock()
			seen[task.ID()] = struct{}{}
			complete := len(seen) == numTasks
			mu.Unlock()
			if complete {
				doneOnce.Do(func() { close(done) })
			}
			return nil
		}),
	)
	require.NoError(t, err)

	hostA := startHost(t, dbPath, poolA)
	startHost(t, dbPath, poolB)

	// Submit through host A's service; placement spreads tasks across both hosts, and any that land on A must re-route to B
	svc := poolA.Service(hostA.Service())
	for range numTasks {
		_, err = svc.Submit(t.Context(), nil)
		require.NoError(t, err)
	}

	select {
	case <-done:
		// Every distinct task ran on host B, which means the ones placed on host A were re-routed rather than failed
	case <-time.After(40 * time.Second):
		t.Fatal("not all tasks were re-routed and run on the accepting host")
	}
}

// assertUnreachable fails the test if the declining host ever runs a task's handler
func assertUnreachable(t *testing.T) error {
	t.Helper()
	t.Error("declining host ran a task it should have rejected")
	return nil
}

// TestCapabilityRouting verifies a task runs on a host that advertises its required capability, and stays pending when no host does
func TestCapabilityRouting(t *testing.T) {
	t.Run("runs on a host advertising the capability", func(t *testing.T) {
		ran := make(chan string, 1)
		handler := func(ctx context.Context, task taskpool.Task) error {
			ran <- task.Capability()
			return nil
		}

		pool, err := taskpool.New("gpuwork", taskpool.WithHandler(handler), taskpool.WithCapability("gpu"))
		require.NoError(t, err)

		host := startHost(t, filepath.Join(t.TempDir(), "cap-yes.db"), pool)
		svc := pool.Service(host.Service())

		_, err = svc.Submit(t.Context(), nil, taskpool.WithRequiredCapability("gpu"))
		require.NoError(t, err)

		select {
		case cap := <-ran:
			assert.Equal(t, "gpu", cap)
		case <-time.After(15 * time.Second):
			t.Fatal("gpu task did not run on a capable host")
		}
	})

	t.Run("stays pending when no host advertises the capability", func(t *testing.T) {
		var baseRan atomic.Bool
		gpuRan := make(chan struct{}, 1)
		handler := func(ctx context.Context, task taskpool.Task) error {
			if task.Capability() == "gpu" {
				gpuRan <- struct{}{}
			} else {
				baseRan.Store(true)
			}
			return nil
		}

		// This host advertises no capability, so it serves only the base queue
		pool, err := taskpool.New("gpuwork", taskpool.WithHandler(handler))
		require.NoError(t, err)

		host := startHost(t, filepath.Join(t.TempDir(), "cap-no.db"), pool)
		svc := pool.Service(host.Service())

		// A base task must run, proving the pool is live
		_, err = svc.Submit(t.Context(), nil)
		require.NoError(t, err)
		// A gpu task has no capable host, so it must not run
		gpuID, err := svc.Submit(t.Context(), nil, taskpool.WithRequiredCapability("gpu"))
		require.NoError(t, err)

		require.Eventually(t, baseRan.Load, 15*time.Second, 100*time.Millisecond, "base task should have run")

		select {
		case <-gpuRan:
			t.Fatal("gpu task ran despite no host advertising the capability")
		case <-time.After(3 * time.Second):
			// The gpu task stayed pending, as expected
		}

		// It is still reported as a live (pending) task
		info, err := svc.GetTask(t.Context(), gpuID)
		require.NoError(t, err)
		assert.Equal(t, taskpool.TaskStatusPending, info.Status)
		assert.Equal(t, "gpu", info.Capability)
	})
}
