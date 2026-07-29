//go:build integration

package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

const (
	// jobMaxAttempts gives the job enough attempts to outlive the induced failures and the host that dies partway through them
	jobMaxAttempts = 8
	// jobRetryDelay keeps the retry cycle short enough to observe within a scenario
	jobRetryDelay = 200 * time.Millisecond
	// jobInducedFailures is how many executions fail before one is allowed to succeed, so the job is still mid-retry when its host is killed
	jobInducedFailures = 3
)

// jobSurvivesHostDeath kills the host retrying a job and verifies the job is picked up elsewhere and run to completion rather than dying with it
//
// The jobs suite covers failures the actor itself returns, where the host stays up throughout and its in-memory retry state is never in doubt
// Retries are held only in memory, so a host that dies mid-cycle takes them with it, and the only thing that can finish the job is another host re-fetching it once the lease expires
type jobSurvivesHostDeath struct {
	cluster *cluster.Cluster
}

func (s *jobSurvivesHostDeath) Name() string {
	return "faults-job-host-death/local/sqlite"
}

func (s *jobSurvivesHostDeath) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    cluster.Local,
		Variant: provider.SQLite,
		Hosts:   2,
		Actors: []frameworkhost.ActorReg{
			shared.ProbeReg(
				actorcore.WithIdleTimeout(time.Minute),
				actorcore.WithMaxAttempts(jobMaxAttempts),
				actorcore.WithInitialRetryDelay(jobRetryDelay),
			),
		},
		HostHealthCheckDeadline: healthCheckDeadline,
		HealthCheckPolicy:       healthCheckPolicy,
		ProviderQueryTimeout:    queryTimeout,
		HostRequestTimeout:      requestTimeout,
		AlarmsPollInterval:      alarmsPollInterval,
		AlarmsLeaseDuration:     alarmsLeaseDuration,
		StallableProvider:       true,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *jobSurvivesHostDeath) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "faults-job-death-1"

	labels := labelHosts(s.cluster)

	// The job fails its first few executions, so it is still being retried when its host is taken away
	shared.ProbeObserver.SetJobFault(actorID, jobInducedFailures)

	_, err := s.cluster.Service(0).Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil)
	require.NoError(t, err)

	// Wait for the first failed execution, which tells us which host holds the job
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.JobCount(actorID) >= 1 && shared.ProbeObserver.LastJobHost(actorID) != ""
	}, 30*time.Second, 50*time.Millisecond, "the job should run at least once before its host is killed")

	owner := shared.ProbeObserver.LastJobHost(actorID)
	ownerIdx := hostIndex(labels, owner)
	require.GreaterOrEqual(t, ownerIdx, 0)
	survivor := (ownerIdx + 1) % s.cluster.Len()

	// Kill the host mid-retry, without letting it release the lease or hand anything over
	s.cluster.StallProvider(t, ownerIdx)
	s.cluster.Host(ownerIdx).Stop(t)
	s.cluster.UnstallProvider(t, ownerIdx)

	// The job is not lost with the host: the survivor re-fetches it once the lease expires and carries it to a successful execution
	require.Eventually(t, func() bool {
		fires := shared.ProbeObserver.JobFires(actorID)
		if len(fires) == 0 {
			return false
		}
		return !fires[len(fires)-1].Failed
	}, recoveryTimeout, recoveryInterval, "the job should reach a successful execution after the host running it died")

	assert.Equal(t, labels[survivor], shared.ProbeObserver.LastJobHost(actorID), "the job should have finished on the surviving host")

	// It was never dead-lettered: the attempts it burned were the induced failures, not the host dying
	assert.Zero(t, shared.ProbeObserver.JobFailedCount(actorID), "losing a host must not dead-letter the job")

	// Every induced failure was delivered, so the retries genuinely continued across the handover rather than restarting from nothing
	fires := shared.ProbeObserver.JobFires(actorID)
	failed := 0
	for _, f := range fires {
		if f.Failed {
			failed++
		}
	}
	assert.Equal(t, jobInducedFailures, failed, "the job should have failed exactly the number of times it was told to")
}
