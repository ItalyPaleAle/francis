---
title: "Checkout with compensations"
weight: 20
description: "The compensation stack, idempotent handlers, and per-step retry policies"
---

A checkout reserves inventory, charges a card, creates a shipment, and confirms. Every step that takes money or holds stock can be undone, and any failure after the charge must refund it. The shipment carrier is a remote service that is sometimes slow.

## The definition

```go
checkout, err := workflow.New("checkout",
	workflow.WithVersion(2),
	workflow.WithTimeout(15*time.Minute),
	workflow.WithRetention(workflow.RetentionPolicy{Completed: 7 * 24 * time.Hour, Failed: 30 * 24 * time.Hour}),
	workflow.WithSteps(
		// Nothing to undo here: validation has no side effects
		workflow.Step("validate", workflow.WithRun(validateCart)),

		workflow.Step("reserve-inventory",
			workflow.WithRun(reserveInventory),
			workflow.WithCompensate(releaseInventory),
		),

		// The charge is the step everything else must be able to undo
		// Its compensation is tried harder than anything else in the graph
		workflow.Step("charge",
			workflow.WithRun(chargeCard),
			workflow.WithCompensate(refundCharge),
			workflow.WithMaxAttempts(3),
			workflow.WithStepTimeout(30*time.Second),
			workflow.WithCompensateMaxAttempts(20),
			workflow.WithCompensateBackoff(10*time.Second, 10*time.Minute),
		),

		// The carrier is slow and flaky; give it room to be, and undo it if a later step fails
		workflow.Step("create-shipment",
			workflow.WithRun(createShipment),
			workflow.WithCompensate(cancelShipment),
			workflow.WithMaxAttempts(6),
			workflow.WithRetryBackoff(5*time.Second, 2*time.Minute),
			workflow.WithStepTimeout(5*time.Minute),
		),

		// Confirmations are best-effort and independent of each other
		workflow.Parallel("confirm",
			workflow.Step("email", workflow.WithRun(sendConfirmationEmail), workflow.WithOptional()),
			workflow.Step("sms", workflow.WithRun(sendConfirmationSMS), workflow.WithOptional()),
		),
	),
)
```

## The handlers

The charge is the one to get right. It uses the instance ID and step name as the payment provider's idempotency key, so an attempt that succeeded but could not report — the host died in between — does not charge twice when it runs again. And it returns the charge ID, because that is what the refund needs:

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

	return chargeResult{ChargeID: ch.ID, Amount: ch.Amount}, nil
}

func refundCharge(ctx context.Context, c workflow.Compensation) error {
	var res chargeResult
	err := c.DecodeResult(&res)
	if err != nil {
		return errors.Join(actor.ErrJobPermanentFailure, err)
	}

	// Refunds are keyed on the charge, so a second delivery of this compensation is a no-op at the provider
	// The cause is recorded with the refund because "the carrier failed" and "the customer cancelled" are different ledger entries
	return payments.Refund(ctx, res.ChargeID, c.Cause())
}
```

The inventory pair follows the same shape: reserve under a deterministic token, release by that token.

```go
type reservationResult struct {
	Token string `json:"token"`
}

func reserveInventory(ctx context.Context, t workflow.Task) (any, error) {
	var order orderInput
	err := t.DecodeInput(&order)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	// Deterministic, so a retried attempt re-reserves the same token rather than holding stock twice
	token := t.InstanceID()
	err = inventory.Reserve(ctx, token, order.Lines)
	if errors.Is(err, inventory.ErrOutOfStock) {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	} else if err != nil {
		return nil, err
	}

	return reservationResult{Token: token}, nil
}

func releaseInventory(ctx context.Context, c workflow.Compensation) error {
	var res reservationResult
	err := c.DecodeResult(&res)
	if err != nil {
		return errors.Join(actor.ErrJobPermanentFailure, err)
	}

	// Releasing a reservation that is already released is a no-op, which is what makes this safe to run twice
	return inventory.Release(ctx, res.Token)
}
```

`createShipment` books the carrier and returns the booking reference; `cancelShipment` cancels by that reference. `validateCart` has no side effects and therefore no compensation at all.

## Starting one

```go
svc := checkout.Service(host.Service())

// Using the order ID as the instance ID makes a retried checkout request find the run already in flight
id, created, err := svc.Start(ctx, order, workflow.WithInstanceID("order-"+order.ID))
if err != nil {
	return err
}
if !created {
	// A run for this order was already started
}
```

## What happens when it goes wrong

**The card is declined.** `charge` reports a permanent failure on its first attempt; the instance unwinds. The stack holds one frame, `reserve-inventory`, so `releaseInventory` runs, and the instance terminates `failed` with `compensation: completed`. Nothing was charged, so nothing is refunded — the failing step is not on the stack.

**The carrier times out for six minutes.** `create-shipment` attempts one through six fail retryable, five seconds, ten, twenty, forty, eighty, and two minutes apart; the sixth failure fails the step. The unwind pops `charge` first — `refundCharge` runs on an undo worker — then `reserve-inventory`. The instance is `failed`, `compensation: completed`, and **the customer was refunded before the stock was released**, in that order, as the stack guarantees.

**The refund itself keeps failing because the payment provider is down.** `refundCharge` is retried up to twenty times over about two hours of backoff. If it never succeeds, `ContinueUnwinding` still releases the inventory and the instance terminates `failed` with `compensation: partial` — the status a "money may be stranded" alert is built on — with the charge ID in the journal for the operator.

**A host dies after the charge succeeded and before the worker reported.** The attempt's job is retried on another host; the handler runs again with the same idempotency key, the provider returns the existing charge, and the report goes out. One charge.

**The customer cancels while the shipment is being created.** `Cancel` moves the instance to `compensating`; the shipment attempt in flight is not interrupted, and if it succeeds its result is recorded and then compensated by `cancelShipment`, along with the refund and the release. The instance is `cancelled`.
