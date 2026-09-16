---
title: "Tenant provisioning"
weight: 30
description: "Waiting on an approval, conditional steps, and a parallel group of child workflows"
---

A new tenant is requested, a manager has to approve it, and then three subsystems are provisioned in parallel — two of them complex enough to be workflows of their own — before the tenant is verified and welcomed. If anything fails after approval, everything provisioned so far is torn down, including whatever the child workflows built.

## The child definitions

Each child keeps its own journal, its own compensation stack, and its own timers. `WithOutput` names the step whose output the parent reads back.

```go
provisionDatabase, err := workflow.New("provision-database",
	workflow.WithOutput("credentials"),
	workflow.WithSteps(
		workflow.Step("create-cluster",
			workflow.WithRun(createCluster),
			workflow.WithCompensate(deleteCluster),
			workflow.WithStepTimeout(10*time.Minute),
			// Only hosts advertising "cloud-api" run this, and its undo runs on one too
			workflow.WithRequiredCapability("cloud-api"),
		),
		workflow.Step("create-schema", workflow.WithRun(createSchema)),
		workflow.Step("credentials",
			workflow.WithRun(issueCredentials),
			workflow.WithCompensate(revokeCredentials),
		),
	),
)

provisionStorage, err := workflow.New("provision-storage",
	workflow.WithOutput("bucket"),
	workflow.WithSteps(
		workflow.Step("bucket", workflow.WithRun(createBucket), workflow.WithCompensate(deleteBucket)),
		workflow.Step("policy", workflow.WithRun(attachPolicy), workflow.WithCompensate(detachPolicy)),
	),
)
```

## The parent

```go
onboarding, err := workflow.New("tenant-onboarding",
	workflow.WithTimeout(7*24*time.Hour),
	workflow.WithSteps(
		// Opens the review ticket
		// Its compensation closes it with the reason
		workflow.Step("request-review",
			workflow.WithRun(openReviewTicket),
			workflow.WithCompensate(closeReviewTicket),
		),

		// Parks the instance until a manager approves, or three days pass
		workflow.WaitForEvent("approval",
			workflow.WithEventTimeout(72*time.Hour),
		),

		// The approval payload says whether it was approved
		workflow.Step("approved", workflow.WithRun(readApproval)),

		// Three subsystems at once, two of them workflows in their own right
		workflow.Parallel("provision",
			workflow.Child("database", workflow.WithDefinition(provisionDatabase)),
			workflow.Child("storage", workflow.WithDefinition(provisionStorage)),
			workflow.Step("dns", workflow.WithRun(createDNS), workflow.WithCompensate(deleteDNS)),
		),

		workflow.Step("verify",
			workflow.WithRun(verifyTenant),
			workflow.WithInputFrom("provision"),
			workflow.WithSkipIf("approved", false),
		),

		workflow.Step("welcome", workflow.WithRun(sendWelcome), workflow.WithOptional()),
	),
)
```

Register all three on every host that should run their steps:

```go
err = host.RegisterBuiltInActor(provisionDatabase)
err = host.RegisterBuiltInActor(provisionStorage)
err = host.RegisterBuiltInActor(onboarding)
```

## The handlers

`readApproval` turns the event payload into the boolean the condition reads:

```go
type approvalPayload struct {
	Approved bool   `json:"approved"`
	By       string `json:"by"`
	Note     string `json:"note,omitempty"`
}

func readApproval(ctx context.Context, t workflow.Task) (any, error) {
	var payload approvalPayload
	err := t.DecodeOutput("approval", &payload)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	// A rejection is a recorded output, which is what WithSkipIf reads on the step below
	return payload.Approved, nil
}
```

`verifyTenant` reads the parallel group's output through `WithInputFrom("provision")`: an object with the database child's credentials under `database`, the storage child's bucket under `storage`, and the DNS record under `dns`.

```go
type provisionOutput struct {
	Database databaseCredentials `json:"database"`
	Storage  bucketRef           `json:"storage"`
	DNS      dnsRecord           `json:"dns"`
}

func verifyTenant(ctx context.Context, t workflow.Task) (any, error) {
	var provisioned provisionOutput
	err := t.DecodeOutput("provision", &provisioned)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	return nil, healthcheck.Tenant(ctx, provisioned.Database, provisioned.Storage, provisioned.DNS)
}
```

## Driving it

```go
svc := onboarding.Service(host.Service())

// One instance per tenant, so a retried request finds the run already in flight
id, _, err := svc.Start(ctx, tenantRequest{Name: name, Plan: plan}, workflow.WithInstanceID(tenantID))
```

The approval arrives from outside — a button in an admin console, a chat command — as a single call:

```go
err = svc.RaiseEvent(ctx, tenantID, "approval", approvalPayload{Approved: true, By: user, Note: note})
```

## What happens when it goes wrong

**Nobody approves within three days.** The event timeout elapses; the instance unwinds. The stack holds one frame, `request-review`, so `closeReviewTicket` runs with the cause `event "approval" timed out`, and the instance terminates `failed`. Nothing was provisioned.

**The manager rejects.** `RaiseEvent` carries `Approved: false`; the wait completes, `readApproval` returns `false`, and `WithSkipIf` skips `verify`. The provision group still runs — in this graph a rejected tenant is still provisioned in a sandbox — and the instance completes with `verify` recorded as `skipped`.

**Storage's `policy` step fails permanently after the database child has already completed.** The storage child unwinds itself (deletes its bucket) and reports failure to the parent; `FailFast` fails the `provision` group. The parent's stack has two frames: the group — which holds the *completed* database child and the DNS record — and `request-review`. Unwinding the group sends an unwind to the database child, which pops its own stack (revokes the credentials, deletes the cluster, on the `cloud-api` hosts it ran on), and runs `deleteDNS`, all concurrently; then the ticket is closed. The parent is `failed`, `compensation: completed`.

```go
// Both children, in their terminal states
page, err := dbSvc.List(ctx, &workflow.ListOptions{Parent: tenantID})
```

**The cloud API is under maintenance mid-provisioning.** An operator suspends the parent:

```go
err = svc.Suspend(ctx, tenantID, "cloud maintenance")
```

That stops it starting anything new. The children keep running to whatever point their own attempts allow, and their reports wait. The seven-day instance timeout is paused for the duration. `Resume` continues from wherever the journal says the instance is.
