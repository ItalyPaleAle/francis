//go:build integration

package jobs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

const (
	immediateJobPollInterval  = time.Minute
	immediateJobStartupSettle = 750 * time.Millisecond
	immediateJobDelay         = 500 * time.Millisecond
	immediateJobTimeout       = 5 * time.Second
	immediateJobTick          = 100 * time.Millisecond
	futureJobDelay            = 4 * time.Second
	futureJobObservation      = futureJobDelay + time.Second
	redispatchJobWait         = 2 * time.Second
	canceledJobDelay          = time.Second
	canceledJobObservation    = 2 * time.Second
)

// Register immediate job leasing for both runtime topologies and every distinct provider implementation
func init() {
	for _, kind := range []cluster.Kind{cluster.Local, cluster.Remote} {
		for _, variant := range []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory} {
			suite.Register(&immediateJobs{kind: kind, variant: variant})
		}
	}
}

// immediateJobs separates immediate leasing from the periodic fetcher with a poll interval much longer than every assertion
type immediateJobs struct {
	kind    cluster.Kind
	variant provider.Variant
	cluster *cluster.Cluster
}

func (s *immediateJobs) Name() string {
	return "jobs/immediate/" + string(s.kind) + "/" + string(s.variant)
}

func (s *immediateJobs) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors: []frameworkhost.ActorReg{
			shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute)),
		},
		AlarmsPollInterval: immediateJobPollInterval,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *immediateJobs) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()

	// Let the one-off startup fetch finish so it cannot satisfy either assertion
	time.Sleep(immediateJobStartupSettle)

	// A job inside fetch-ahead must execute without waiting for the one-minute periodic poll
	t.Run("inside fetch-ahead executes immediately", func(t *testing.T) {
		actorID := "immediate-" + string(s.kind) + "-" + string(s.variant)
		jobData := "pre-leased"
		_, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", jobData, actor.WithJobDelay(immediateJobDelay))
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return shared.ProbeObserver.JobCount(actorID) > 0
		}, immediateJobTimeout, immediateJobTick, "a job inside fetch-ahead should execute before the periodic poll")

		fires := shared.ProbeObserver.JobFires(actorID)
		require.NotEmpty(t, fires)
		assert.Equal(t, "process", fires[0].Method)
		assert.Equal(t, jobData, fires[0].Data)
	})

	// A job outside fetch-ahead must remain stored until a later poll fetches it
	t.Run("outside fetch-ahead is not enqueued", func(t *testing.T) {
		actorID := "future-" + string(s.kind) + "-" + string(s.variant)
		jobID, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil, actor.WithJobDelay(futureJobDelay))
		require.NoError(t, err)

		neverFired := assert.Never(t, func() bool {
			return shared.ProbeObserver.JobCount(actorID) > 0
		}, futureJobObservation, immediateJobTick, "a job outside fetch-ahead should not execute before the periodic poll")
		if !neverFired {
			return
		}

		err = svc.CancelJob(ctx, shared.ProbeActorType, actorID, jobID)
		require.NoError(t, err)
	})

	// Re-dispatching after the stored occurrence enters fetch-ahead must lease the first-write-wins row
	t.Run("re-dispatch leases the stored occurrence", func(t *testing.T) {
		actorID := "redispatch-" + string(s.kind) + "-" + string(s.variant)
		jobID, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", "original", actor.WithJobDelay(futureJobDelay), actor.WithIdempotencyKey("same-key"))
		require.NoError(t, err)

		// Wait until the original due time is inside fetch-ahead while the periodic poll remains idle
		time.Sleep(redispatchJobWait)
		duplicateID, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", "replacement", actor.WithIdempotencyKey("same-key"))
		require.NoError(t, err)
		assert.Equal(t, jobID, duplicateID)

		require.Eventually(t, func() bool {
			return shared.ProbeObserver.JobCount(actorID) > 0
		}, immediateJobTimeout, immediateJobTick, "the retained occurrence should execute without a periodic poll")
		fires := shared.ProbeObserver.JobFires(actorID)
		require.NotEmpty(t, fires)
		assert.Equal(t, "original", fires[0].Data)
	})

	// Cancelling a pre-leased job invalidates the queued lease before it can execute
	t.Run("cancel pre-leased job", func(t *testing.T) {
		actorID := "cancel-preleased-" + string(s.kind) + "-" + string(s.variant)
		jobID, err := svc.Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil, actor.WithJobDelay(canceledJobDelay))
		require.NoError(t, err)
		err = svc.CancelJob(ctx, shared.ProbeActorType, actorID, jobID)
		require.NoError(t, err)

		assert.Never(t, func() bool {
			return shared.ProbeObserver.JobCount(actorID) > 0
		}, canceledJobObservation, immediateJobTick, "a canceled pre-leased job must not execute")
	})
}
