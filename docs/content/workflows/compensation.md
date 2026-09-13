---
title: "Compensation"
weight: 40
description: "Undoing what already succeeded, in reverse order"
---

A **compensation** is a per-step callback that undoes the effect of a task that completed successfully. It is what turns a sequence of steps into a saga: when the workflow cannot finish, the work it already did is unwound rather than left stranded.

```go
workflow.Step("charge",
	workflow.WithRun(chargeCard),
	workflow.WithCompensate(refundCharge),
),
```

## The stack

Compensation is a **stack**. Every task that completes successfully, and whose step declares `WithCompensate` — or whose step is a child workflow — is pushed onto the journal's compensation stack in completion order. When the instance has to unwind, the stack is popped in reverse.

```
forward:      charge-card ──► reserve-stock ──► ship[0] ship[1] ship[2] ──► ✗ confirm
                                               (parallel children)

unwind:       refund-card ◄── release-stock ◄── unwind ship[0..2]
                                               (parallel, in one frame)
```

Three ordering rules:

- **Frames unwind in reverse order.** A step that ran after another is compensated before it. That is the invariant a saga depends on: the customer is refunded before the stock is released, because the stock was held first.
- **Within a frame, compensations run concurrently.** The tasks of a parallel group or fan-out had no order between them going forward, so imposing one on the way back would only make unwinding slower.
- **A frame is fully compensated before the next one starts.** That is the point of unwinding in reverse.

Every compensation is a task on an undo worker, like every other unit of work. The unwind is scheduled by the `Workflow` actor and performed nowhere near it.

## What triggers an unwind

- A step fails terminally under the default policy (not `WithOptional` or `WithSkipOnFailure`).
- A group or fan-out fails under `FailFast` or `CollectFailures`.
- `Service.Cancel` is called on a running or suspended instance.
- The instance timeout elapses.
- A `WaitForEvent` step's own timeout elapses without the event.
- A parent instance unwinds a [child step](/workflows/child-workflows).

In every case the journal records the **cause**, which each compensation receives through `Cause()`. A compensation frequently needs it: "release the stock because the payment failed" and "release the stock because the customer cancelled" may write different audit records.

## Writing one

A compensation receives the same input and item the forward task had, plus the **output that task produced** — which is usually what identifies the effect to undo:

```go
type chargeResult struct {
	ChargeID string `json:"chargeId"`
	Amount   int64  `json:"amount"`
}

func chargeCard(ctx context.Context, t workflow.Task) (any, error) {
	var order orderInput
	err := t.DecodeInput(&order)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	// The same key on every attempt means the provider returns the existing charge instead of creating another
	idem := t.InstanceID() + "|" + t.Step()
	ch, err := payments.Charge(ctx, order.PaymentMethod, order.Total, idem)
	if errors.Is(err, payments.ErrDeclined) {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	} else if err != nil {
		return nil, err
	}

	// Return the charge ID, because that is what the refund needs
	return chargeResult{ChargeID: ch.ID, Amount: ch.Amount}, nil
}

func refundCharge(ctx context.Context, c workflow.Compensation) error {
	var res chargeResult
	err := c.DecodeResult(&res)
	if err != nil {
		return errors.Join(actor.ErrJobPermanentFailure, err)
	}

	// Refunds are keyed on the charge, so a second delivery of this compensation is a no-op at the provider
	return payments.Refund(ctx, res.ChargeID, c.Cause())
}
```

Compensations are **at-least-once**, like everything else, so `refundCharge` must tolerate being called twice for the same charge. In practice that means keying the undo on the forward operation's identifier, which the handler already has.

Compensations get their own attempt policy — `WithCompensateMaxAttempts` (default 10) and `WithCompensateBackoff` (default 10s doubling to 10 minutes) — recorded in the journal exactly like forward attempts. The default is more generous than the forward policy because a failed rollback leaves the system inconsistent, so it is worth trying harder.

## The failing step itself

By default, a step that **failed** is not compensated: the saga convention is that a step which did not complete did not take effect. That is a convention, not a guarantee — a step can fail after its side effect landed and before it reported.

`WithCompensateOnFailure()` opts a step into being compensated even when it failed, for handlers whose effect may be partial:

```go
workflow.Step("write-ledger",
	workflow.WithRun(writeLedger),
	workflow.WithCompensate(reverseLedger),
	// The write may have landed before the failure, so the undo runs either way
	workflow.WithCompensateOnFailure(),
),
```

Such a compensation must be written defensively: it may be undoing something that never happened.

## When a compensation fails

`WithCompensationFailurePolicy` chooses what happens next:

| Policy | Behavior |
|--------|----------|
| `workflow.ContinueUnwinding` *(default)* | Record the failure and keep unwinding the remaining frames. The instance terminates as `failed` with `compensation: partial`. |
| `workflow.AbortUnwinding` | Stop at the failed frame. The instance terminates as `failed` with `compensation: failed`, and the journal names exactly which frames were not unwound. |

`ContinueUnwinding` is the default because stopping at the first problem usually leaves **more** state stranded than continuing does, and because the alternative is an instance sitting in `compensating` waiting for a human.

Either way the outcome is explicit in the status, which is what an alert should be built on. Neither policy silently succeeds: a workflow whose rollback did not complete is never reported as cleanly rolled back.

```go
status, err := svc.GetStatus(ctx, id)
if status.Compensation == workflow.CompensationPartial {
	// Money may be stranded; the charge ID is in the journal for the operator
}
```

## Status

The instance carries a status plus a compensation outcome, rather than multiplying terminal statuses:

```go
const (
	CompensationNone      CompensationOutcome = "none"      // nothing needed unwinding
	CompensationCompleted CompensationOutcome = "completed" // every frame unwound
	CompensationPartial   CompensationOutcome = "partial"   // some frames failed, the rest were unwound
	CompensationFailed    CompensationOutcome = "failed"    // the unwind stopped early
)
```

The outcome is empty until the instance reaches a terminal status. While it is `compensating`, the per-step view says how far the unwind has got: a step is `compensating`, `compensated`, or `compensation-failed`.

An instance with nothing on its stack passes through `compensating` in a single turn, so the path is uniform whether or not anything has to be undone.
