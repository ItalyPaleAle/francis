// This code was adapted from https://github.com/dapr/kit/tree/v0.15.4/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

package eventqueue

import (
	"fmt"
	"time"
)

// queueableItem is an item that can be queued and it's used for testing.
type queueableItem struct {
	Name          string
	ExecutionTime time.Time
}

// Key returns the key for this unique item.
func (r queueableItem) Key() string {
	return r.Name
}

// DueTime returns the time the item is due to be executed at.
// This is implemented to comply with the queueable interface.
func (r queueableItem) DueTime() time.Time {
	return r.ExecutionTime
}

//nolint:errcheck
func ExampleProcessor() {
	// Method invoked when an item is to be executed
	executed := make(chan string, 3)
	executeFn := func(r *queueableItem) {
		executed <- "Executed: " + r.Name
	}

	// Create the processor
	processor := NewProcessor(Options[string, *queueableItem]{
		ExecuteFn: executeFn,
	})

	// Add items to the processor, in any order, using Enqueue
	_ = processor.Enqueue(&queueableItem{Name: "item1", ExecutionTime: time.Now().Add(500 * time.Millisecond)})
	_ = processor.Enqueue(&queueableItem{Name: "item2", ExecutionTime: time.Now().Add(200 * time.Millisecond)})
	_ = processor.Enqueue(&queueableItem{Name: "item3", ExecutionTime: time.Now().Add(300 * time.Millisecond)})
	_ = processor.Enqueue(&queueableItem{Name: "item4", ExecutionTime: time.Now().Add(time.Second)})

	// Items with the same value returned by Key() are considered the same, so will be replaced
	_ = processor.Enqueue(&queueableItem{Name: "item3", ExecutionTime: time.Now().Add(100 * time.Millisecond)})

	// Using Dequeue allows removing an item from the queue
	processor.Dequeue("item4")

	for range 3 {
		fmt.Println(<-executed)
	}
	// Output:
	// Executed: item3
	// Executed: item2
	// Executed: item1
}
