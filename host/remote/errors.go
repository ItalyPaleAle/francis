package remote

import (
	"errors"

	"github.com/italypaleale/francis/protocol"
)

// isProtocolErrorCode reports whether err is a protocol error carrying the given code
func isProtocolErrorCode(err error, code protocol.ErrorCode) bool {
	if err == nil {
		return false
	}

	perr, ok := errors.AsType[*protocol.Error](err)
	if !ok {
		return false
	}
	return perr.Code == code
}
