---
title: "Tenant provisioning"
weight: 30
---

This is an example of an IT workflow. When a new tenant is requested, a manager has to approve it, and three subsystems are provisioned in parallel before the tenant is verified and welcomed. Two of the three are complex enough to be workflows of their own. If anything fails after approval, everything provisioned so far is torn down, including whatever the children built.

## Children workflow definitions

Each child keeps its own status, compensation stack, and timers. `WithOutput` names the step whose output the parent reads back.

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

## Parent workflow definition

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

## Handlers

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

`verifyTenant` reads the parallel group's output through `WithInputFrom("provision")`: the database child's credentials under `database`, the storage child's bucket under `storage`, and the DNS record under `dns`.

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

## Starting and driving the workflow

```go
svc := onboarding.Service(host.Service())

// One instance per tenant, so a retried request finds the run already in flight
id, _, err := svc.Start(ctx, tenantRequest{Name: name, Plan: plan}, workflow.WithInstanceID(tenantID))
```

When the workflow waits for the external approval, notify the manager in some ways (e.g. an email, a Slack/Teams notification, etc). The approval arrives from outside, as a single call, for example through a webhook that's invoked from the Slack/Teams message:

```go
err = svc.RaiseEvent(ctx, tenantID, "approval", approvalPayload{Approved: true, By: user, Note: note})
```

## Example failures

Example failures and how they are handled:

- Nobody approves within three days.  
   The event timeout elapses. The stack holds one entry, `request-review`, so `closeReviewTicket` runs with the cause `event "approval" timed out` and the instance terminates `failed`. Nothing was provisioned.
- The manager rejects.  
   `RaiseEvent` carries `Approved: false`, the wait completes, `readApproval` returns `false`, and `WithSkipIf` skips `verify`. The provision group still runs, since in this graph a rejected tenant is provisioned in a sandbox, and the instance completes with `verify` recorded as `skipped`.
- Storage's `policy` step fails permanently after the database child has completed.  
   The storage child deletes its bucket and reports failure, and `FailFast` fails the `provision` group.
- The parent's stack holds the group and `request-review`.  
   Rolling back the group asks the completed database child to undo itself, which revokes its credentials and deletes its cluster, and runs `deleteDNS` at the same time. The ticket is closed last. The parent is `failed` with `compensation: completed`.

   ```go
   // Both children, in their terminal states
   page, err := dbSvc.List(ctx, &workflow.ListOptions{Parent: tenantID})
   ```

- The cloud API is under maintenance mid-provisioning.  
   An operator suspends the parent:

   ```go
   err = svc.Suspend(ctx, tenantID, "cloud maintenance")
   ```

   That stops it starting anything new. The children keep running, and their results wait. The seven-day instance timeout is paused, and `Resume` continues where the instance left off.
