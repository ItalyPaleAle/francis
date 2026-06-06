package peer

import (
	"errors"
	"io"
)

// errRequestBodyTooLarge is returned once a streamed request body exceeds the configured limit
var errRequestBodyTooLarge = errors.New("request body exceeds the maximum allowed size")

// maxBytesReader wraps a reader and returns errRequestBodyTooLarge once more than max bytes have been read
// It mirrors net/http.MaxBytesReader: bytes are yielded up to the limit, then the read that crosses it fails, rather than silently truncating the body
type maxBytesReader struct {
	r   io.Reader
	n   int64 // bytes remaining before the limit is exceeded
	err error
}

// newMaxBytesReader wraps r so reads fail once more than max bytes are consumed
func newMaxBytesReader(r io.Reader, max int64) *maxBytesReader {
	return &maxBytesReader{r: r, n: max}
}

func (l *maxBytesReader) Read(p []byte) (int, error) {
	if l.err != nil {
		return 0, l.err
	}
	if len(p) == 0 {
		return 0, nil
	}

	// Read at most one byte past the remaining budget, so an over-limit body is detected rather than silently truncated
	if int64(len(p)) > l.n+1 {
		p = p[:l.n+1]
	}

	n, err := l.r.Read(p)
	if int64(n) <= l.n {
		l.n -= int64(n)
		l.err = err
		return n, err
	}

	// The read crossed the limit: hand back only the bytes within budget and fail
	n = int(l.n)
	l.n = 0
	l.err = errRequestBodyTooLarge
	return n, l.err
}
