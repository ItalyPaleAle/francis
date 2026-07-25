package types

import (
	msgpack "github.com/vmihailenco/msgpack/v5"
)

// MsgpackEnvelope implements actor.Envelope on top of a MessagePack-encoded byte slice.
type MsgpackEnvelope []byte

// Decode the payload into the given object.
func (e MsgpackEnvelope) Decode(into any) error {
	return msgpack.Unmarshal(e, into)
}
