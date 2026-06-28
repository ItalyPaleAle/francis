---
title: "Alarms"
weight: 26
---

An **alarm** is a durable, scheduled callback attached to an actor. When an alarm is triggered, Francis activates the actor (if it isn't already) and calls its `Alarm` method. Alarms are stored in the database, so they survive process restarts and are delivered even if the actor was idle at the scheduled time.

Alarms are how you run work later: retries, timeouts, reminders, periodic maintenance, and so on.

## Receiving alarms

To receive alarms, an actor must implement the `actor.ActorAlarm` interface:

```go
func (c *Cart) Alarm(ctx context.Context, name string, data actor.Envelope) error {
	// "name" identifies which alarm fired
	// "data" is the optional payload attached when the alarm was scheduled (may be nil)
	switch name {
	case "abandon-reminder":
		// ... send a reminder ...
	}
	return nil
}
```

- `name` is the alarm name you chose when scheduling it. An actor can have many alarms with different names.
- `data` is an `actor.Envelope`. Call `data.Decode(&dest)` to read the payload. It may be `nil`.
- Returning an error causes the alarm execution to be retried, according to the actor type's retry settings.

## Scheduling an alarm

Use the client (from inside the actor) or the service to create or replace an alarm:

```go
err := c.client.SetAlarm(ctx, "abandon-reminder", actor.AlarmProperties{
	DueTime: time.Now().Add(1 * time.Hour),
})
```

Setting an alarm with a name that already exists replaces it.

### Alarm properties

`actor.AlarmProperties` describes when and how an alarm is triggered:

| Field | Type | Description |
|-------|------|-------------|
| `DueTime` | `time.Time` | When the alarm is first triggered (absolute time). |
| `Interval` | `string` | Optional repeat interval. A Go duration (`"60s"`), an ISO 8601 duration, or a number of milliseconds. Empty means fire once. |
| `TTL` | `time.Time` | Optional expiration for a repeating alarm: it stops repeating after this time. |
| `Data` | `any` | Optional payload, serialized and delivered to `Alarm` as the `data` envelope. |

### One-off alarm

```go
err := c.client.SetAlarm(ctx, "timeout", actor.AlarmProperties{
	DueTime: time.Now().Add(30 * time.Second),
	Data:    map[string]any{"reason": "checkout-timeout"},
})
```

### Repeating alarm

A repeating alarm fires at `DueTime` and then every `Interval` until its `TTL` (if set):

```go
err := c.client.SetAlarm(ctx, "daily-rollup", actor.AlarmProperties{
	DueTime:  time.Now(),          // fire right away, then repeat
	Interval: "24h",               // every 24 hours
	TTL:      time.Now().Add(30 * 24 * time.Hour), // stop after 30 days
})
```

## Deleting an alarm

```go
err := c.client.DeleteAlarm(ctx, "daily-rollup")
```

`DeleteAlarm` returns `actor.ErrAlarmNotFound` if no alarm with that name exists.

## Managing alarms through the service

As with state, the client is a convenience over `actor.Service`, which works with any actor:

```go
err := service.SetAlarm(ctx, "cart", "user-42", "abandon-reminder", actor.AlarmProperties{
	DueTime: time.Now().Add(time.Hour),
})

err = service.DeleteAlarm(ctx, "cart", "user-42", "abandon-reminder")
```

This is useful for scheduling work on an actor from outside it — for example, from an HTTP handler.

## Delivery semantics

- **Durable**: alarms are persisted, so they survive restarts and are delivered after downtime.
- **Leased execution**: hosts lease alarms from the data store before running them, so an alarm isn't executed by two hosts at once.
- **Activation on trigger**: when an alarm is due, Francis activates the target actor if it isn't already active, then calls `Alarm`.
- **At-least-once with retries**: if `Alarm` returns an error, execution is retried per the actor type's `MaxAttempts` and `InitialRetryDelay`. Design alarm handlers to be idempotent.

## A worked example

Here's a cart that sets a reminder when an item is added, then clears it on checkout:

```go
func (c *Cart) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	switch method {
	case "add-item":
		// ... update state ...
		// Remind the user in 24h if they haven't checked out
		// By replacing previous alarms with the same name, this resets the deadline
		return nil, c.client.SetAlarm(ctx, "abandon-reminder", actor.AlarmProperties{
			DueTime: time.Now().Add(24 * time.Hour),
		})
	case "checkout":
		// ... finalize ...
		// Cancel the reminder
		err := c.client.DeleteAlarm(ctx, "abandon-reminder")
		if err != nil && !errors.Is(err, actor.ErrAlarmNotFound) {
			return nil, err
		}
		return nil, nil
	}
	return nil, nil
}

func (c *Cart) Alarm(ctx context.Context, name string, data actor.Envelope) error {
	if name == "abandon-reminder" {
		// ... notify the user about their abandoned cart ...
	}
	return nil
}
```
