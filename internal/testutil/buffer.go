package testutil

import (
	"bytes"
	"sync"
)

// ConcurrentBuffer is a bytes.Buffer that's safe for concurrent use.
type ConcurrentBuffer struct {
	b bytes.Buffer
	m sync.Mutex
}

func (cb *ConcurrentBuffer) Write(p []byte) (n int, err error) {
	cb.m.Lock()
	defer cb.m.Unlock()
	return cb.b.Write(p)
}

func (cb *ConcurrentBuffer) Read(p []byte) (n int, err error) {
	cb.m.Lock()
	defer cb.m.Unlock()
	return cb.b.Read(p)
}

func (cb *ConcurrentBuffer) String() string {
	cb.m.Lock()
	defer cb.m.Unlock()
	return cb.b.String()
}

func (cb *ConcurrentBuffer) Reset() {
	cb.m.Lock()
	defer cb.m.Unlock()
	cb.b.Reset()
}
