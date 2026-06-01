package runtime

import (
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

// protocolActorTypesToComponents converts protocol actor host type DTOs to their components equivalents
// The protocol expresses durations in milliseconds, which are converted back to time.Duration here
func protocolActorTypesToComponents(in []protocol.ActorHostType) []components.ActorHostType {
	if len(in) == 0 {
		return nil
	}

	out := make([]components.ActorHostType, len(in))
	for i, t := range in {
		out[i] = components.ActorHostType{
			ActorType:           t.ActorType,
			IdleTimeout:         time.Duration(t.IdleTimeoutMs) * time.Millisecond,
			ConcurrencyLimit:    t.ConcurrencyLimit,
			DeactivationTimeout: time.Duration(t.DeactivationTimeoutMs) * time.Millisecond,
			MaxAttempts:         t.MaxAttempts,
			InitialRetryDelay:   time.Duration(t.InitialRetryDelayMs) * time.Millisecond,
		}
	}
	return out
}
