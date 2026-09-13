---
title: "Thumbnails and a manifest"
weight: 10
description: "Fan-out, tolerated failures, and three different costs of failing"
---

An upload becomes a set of thumbnails in several formats and sizes, a manifest describing what was produced, and a notification to the service that asked.

Three steps, three different costs when they fail:

- A **thumbnail** that cannot be produced is recorded, not fatal.
- A **manifest** that cannot be stored is fatal, and there is nothing to notify about without it.
- A **notification** that cannot be delivered is logged, and the run still counts.

## The definition

```go
thumbnails, err := workflow.New("thumbnails",
	workflow.WithTimeout(10*time.Minute),
	workflow.WithRetention(workflow.RetentionPolicy{Completed: 24 * time.Hour, Failed: 72 * time.Hour}),
	workflow.WithAutoPurge("@hourly"),
	// Encoding is the expensive part of a worker, so this is the number of encoders per host
	workflow.WithConcurrency(runtime.NumCPU()),
	workflow.WithSteps(
		// Normalizes the request into one item per thumbnail to produce
		workflow.Step("plan", workflow.WithRun(planThumbnails)),

		// One task per thumbnail; one that cannot be encoded is recorded and does not stop the run
		workflow.ForEach("generate",
			workflow.WithItemsFrom("plan"),
			workflow.WithRun(generateThumbnail),
			workflow.WithMaxAttempts(3),
			workflow.WithFailurePolicy(workflow.TolerateFailures),
		),

		// The manifest is the durable record of the run; without it there is nothing to notify about
		workflow.Step("manifest",
			workflow.WithRun(writeManifest),
			workflow.WithMaxAttempts(5),
			workflow.WithSkipOnFailure("notify"),
		),

		// The thumbnails and the manifest are in the store either way, so a lost notification does not fail the run
		workflow.Step("notify",
			workflow.WithRun(deliverNotification),
			workflow.WithMaxAttempts(10),
			workflow.WithRetryBackoff(5*time.Second, 5*time.Minute),
			workflow.WithOptional(),
		),
	),
)
```

Nothing here needs a compensation. A partial result in an object store is harmless, and the manifest records what failed.

## The handlers

`planThumbnails` turns one request into the list the fan-out iterates:

```go
type uploadRequest struct {
	SourceKey string          `json:"sourceKey"`
	Formats   []string        `json:"formats"`
	Sizes     [][2]int        `json:"sizes"`
	Callback  string          `json:"callback"`
}

type thumbnailSpec struct {
	Format string `json:"format"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
}

func planThumbnails(ctx context.Context, t workflow.Task) (any, error) {
	var in uploadRequest
	err := t.DecodeInput(&in)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	specs := make([]thumbnailSpec, 0, len(in.Formats)*len(in.Sizes))
	for _, format := range in.Formats {
		for _, size := range in.Sizes {
			specs = append(specs, thumbnailSpec{Format: format, Width: size[0], Height: size[1]})
		}
	}

	// The fan-out is sized from this array, and the items are journaled when this step reports
	return specs, nil
}
```

`generateThumbnail` reads its item, does the work, and returns a small handle — never the bytes:

```go
type thumbnailResult struct {
	Key    string `json:"key"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	Size   int64  `json:"size"`
}

func generateThumbnail(ctx context.Context, t workflow.Task) (any, error) {
	var in uploadRequest
	err := t.DecodeInput(&in)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	var spec thumbnailSpec
	err = t.DecodeItem(&spec)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	// A source that cannot be decoded fails the same way on every attempt, so it is reported permanently
	// A store that is briefly unavailable recovers, so that error is returned as-is and retried
	src, err := store.ReadOriginal(ctx, in.SourceKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	} else if err != nil {
		return nil, err
	}

	img, err := encoder.Encode(src, spec)
	if errors.Is(err, encoder.ErrUnsupportedSource) {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	} else if err != nil {
		return nil, err
	}

	// The key is deterministic, so a re-run overwrites the same object rather than leaving a duplicate
	key := thumbnailKey(t.InstanceID(), t.Index(), spec)
	err = store.Write(ctx, key, img.Data, img.ContentType)
	if err != nil {
		return nil, err
	}

	return thumbnailResult{Key: key, Width: img.Width, Height: img.Height, Size: int64(len(img.Data))}, nil
}
```

`writeManifest` reads the fan-out's output — an array with `{"error": …}` in the slots that failed — and stores the manifest under a key derived from the instance ID, so a retried write stores the same object:

```go
func writeManifest(ctx context.Context, t workflow.Task) (any, error) {
	var results []json.RawMessage
	err := t.DecodeOutput("generate", &results)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	manifest := manifestDoc{InstanceID: t.InstanceID()}
	for _, raw := range results {
		var thumb thumbnailResult
		dErr := json.Unmarshal(raw, &thumb)
		if dErr == nil && thumb.Key != "" {
			manifest.Produced = append(manifest.Produced, thumb)
			continue
		}

		// A slot that is not a result is a failure, and the manifest is where it is recorded
		var failure struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(raw, &failure)
		manifest.Failed = append(manifest.Failed, failure.Error)
	}

	key := manifestKey(t.InstanceID())
	body, err := json.Marshal(manifest)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	err = store.Write(ctx, key, body, "application/json")
	if err != nil {
		return nil, err
	}

	return manifestResult{Key: key, Produced: len(manifest.Produced), Failed: len(manifest.Failed)}, nil
}
```

`deliverNotification` posts the manifest key to the callback and treats only a 2xx as delivered:

```go
func deliverNotification(ctx context.Context, t workflow.Task) (any, error) {
	var in uploadRequest
	err := t.DecodeInput(&in)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	var manifest manifestResult
	err = t.DecodeOutput("manifest", &manifest)
	if err != nil {
		return nil, errors.Join(actor.ErrJobPermanentFailure, err)
	}

	status, err := postJSON(ctx, in.Callback, manifest)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("callback returned %d", status)
	}

	return nil, nil
}
```

## Starting one

```go
svc := thumbnails.Service(host.Service())

id, _, err := svc.Start(ctx, uploadRequest{
	SourceKey: "uploads/abc.jpg",
	Formats:   []string{"webp", "avif"},
	Sizes:     [][2]int{{320, 240}, {640, 480}, {1280, 960}},
	Callback:  "https://example.com/hooks/thumbnails",
})
```

## What happens when it goes wrong

**One thumbnail's format is unsupported.** Its attempt reports a permanent failure; `TolerateFailures` records it and the group completes. The manifest lists it with its error, the notification goes out, and the instance is `completed` — `GetStatus` shows the failed task inside a completed step.

**The object store is unreachable for twenty seconds during the fan-out.** Every in-flight attempt reports a retryable error; the orchestrator schedules second attempts two seconds out, and third attempts four seconds out if needed. The journal shows `attempts: 2` or `3` on the affected tasks and the run completes a little later.

**The manifest store is down for longer than five attempts cover.** The step fails; `WithSkipOnFailure` records `notify` as skipped; the instance terminates `failed`, with no unwind, because there is nothing to undo. `List(Status: failed)` finds it, and a new instance with the same input re-drives it once the store is back.

**The callback endpoint returns 503 for an hour.** The notification step exhausts its ten attempts over about twenty-five minutes of backoff; `WithOptional` records the failure and the instance is `completed`. The operator sees the failed optional step in status and in the step-failure metric.
