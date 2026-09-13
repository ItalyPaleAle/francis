package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/builtinactor"
)

// TestRegistrationsCoverEveryReservedType pins down exactly what a workflow registers on a host, since those names are the reserved contract an operator sees in logs, traces, and placement
func TestRegistrationsCoverEveryReservedType(t *testing.T) {
	child, err := New("child", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	wf, err := New("orders",
		WithVersion(2),
		WithConcurrency(4),
		WithCompensateConcurrency(2),
		WithCapability("gpu"),
		WithAutoPurge("0 3 * * *"),
		WithTimeout(time.Hour),
		WithSteps(
			Step("a", WithRun(noopRun), WithCompensate(noopCompensate), WithRequiredCapability("gpu")),
			Child("b", WithDefinition(child)),
		),
	)
	require.NoError(t, err)

	got := map[string]builtinactor.BuiltInActorRegistration{}
	for _, reg := range wf.Registrations() {
		got[builtinactor.FullActorType(reg.ActorType)] = reg
	}

	// The orchestrator, a worker and undo queue per capability plus the base ones, the registry, and the auto-purge cron job
	want := []string{
		"francis.builtin.workflow.orders",
		"francis.builtin.workflow.orders.worker",
		"francis.builtin.workflow.orders.worker.gpu",
		"francis.builtin.workflow.orders.undo",
		"francis.builtin.workflow.orders.undo.gpu",
		"francis.builtin.workflow.orders.registry",
		"francis.builtin.cronjob.orders.purge",
	}
	for _, name := range want {
		assert.Contains(t, got, name)
	}
	assert.Len(t, got, len(want), "a workflow should register exactly these types")

	// The orchestrator is reached at the instance ID, and its generous retry policy is what keeps a database blip from dead-lettering a report
	orchestrator := got["francis.builtin.workflow.orders"]
	assert.False(t, orchestrator.Singleton)
	assert.Equal(t, orchestratorMaxAttempts, orchestrator.RegisterOptions.MaxAttempts)
	assert.Equal(t, orchestratorRetryDelay, orchestrator.RegisterOptions.InitialRetryDelay)
	assert.Equal(t, wf.ActorType(), "workflow.orders")

	// Every worker queue shares one strict per-host budget, and the undo queues form a second one so an unwind cannot starve forward work
	workerGroup := got["francis.builtin.workflow.orders.worker"].RegisterOptions
	assert.Equal(t, 4, workerGroup.CapacityGroupLimit)
	assert.Equal(t, 4, workerGroup.ConcurrencyLimit)
	assert.Equal(t, workerGroup.CapacityGroup, got["francis.builtin.workflow.orders.worker.gpu"].RegisterOptions.CapacityGroup)

	undoGroup := got["francis.builtin.workflow.orders.undo"].RegisterOptions
	assert.Equal(t, 2, undoGroup.CapacityGroupLimit)
	assert.NotEqual(t, workerGroup.CapacityGroup, undoGroup.CapacityGroup, "the undo queues get their own budget")

	// The auto-purge cron job is the cluster-wide singleton that runs the sweep on one host per schedule
	assert.True(t, got["francis.builtin.cronjob.orders.purge"].Singleton)

	// A workflow with no capabilities and no auto-purge registers only the base set
	plain, err := New("plain", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)
	assert.Len(t, plain.Registrations(), 4)
	assert.Equal(t, defaultVersion, plain.Version())
	assert.Equal(t, "plain", plain.Name())
}
