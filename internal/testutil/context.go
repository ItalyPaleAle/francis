//go:build unit

package testutil

import (
	"context"
	"sync/atomic"

	"github.com/italypaleale/actors/internal/ptr"
)

type contextDoneNotifier struct {
	context.Context
	doneCalled *atomic.Pointer[chan struct{}]
}

func NewContextDoneNotifier(parentCtx context.Context) *contextDoneNotifier {
	doneCalled := &atomic.Pointer[chan struct{}]{}
	doneCalled.Store(ptr.Of(make(chan struct{})))
	return &contextDoneNotifier{
		Context:    parentCtx,
		doneCalled: doneCalled,
	}
}

func (c *contextDoneNotifier) WaitForDone() {
	doneCalled := c.doneCalled.Load()
	if doneCalled != nil {
		<-*doneCalled
	}
}

func (c *contextDoneNotifier) Done() <-chan struct{} {
	doneCalled := c.doneCalled.Swap(nil)
	if doneCalled != nil {
		close(*doneCalled)
	}
	return c.Context.Done()
}
