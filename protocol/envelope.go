package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

// ProtocolVersion is the current version of the wire protocol
// During host registration both ends negotiate down to the lowest commonly-supported version
const ProtocolVersion uint16 = 1

// Size limits for protocol messages
const (
	// MaxMessageSize is the maximum size of a single framed envelope on a stream
	// This bounds the in-memory buffer used when reading a message off the wire
	MaxMessageSize = 16 << 20 // 16 MiB
	// MaxPayloadSize is the maximum size of an envelope payload
	// It leaves headroom below MaxMessageSize for the envelope metadata itself
	MaxPayloadSize = MaxMessageSize - (64 << 10)
)

// Message kinds, grouped by traffic path so the allowed direction is obvious at a glance
// Every request has a matching response kind; a response is correlated to its request via CorrelationID
// Errors are reported with KindError regardless of the request, so a receiver can classify a reply as success-vs-error from the Kind alone and only then decode the operation-specific payload
const (
	// Host -> Runtime requests

	KindRegisterHost   = "host.register"
	KindUnregisterHost = "host.unregister"
	KindHealthCheck    = "host.healthcheck"
	KindLookupActor    = "host.lookup"
	KindGetAlarm       = "host.alarm.get"
	KindSetAlarm       = "host.alarm.set"
	KindDeleteAlarm    = "host.alarm.delete"
	KindGetState       = "host.state.get"
	KindSetState       = "host.state.set"
	KindDeleteState    = "host.state.delete"

	// Host -> Runtime responses

	KindRegisterHostResponse   = "host.register.response"
	KindUnregisterHostResponse = "host.unregister.response"
	KindHealthCheckResponse    = "host.healthcheck.response"
	KindLookupActorResponse    = "host.lookup.response"
	KindGetAlarmResponse       = "host.alarm.get.response"
	KindSetAlarmResponse       = "host.alarm.set.response"
	KindDeleteAlarmResponse    = "host.alarm.delete.response"
	KindGetStateResponse       = "host.state.get.response"
	KindSetStateResponse       = "host.state.set.response"
	KindDeleteStateResponse    = "host.state.delete.response"

	// Runtime -> Host requests

	KindExecuteAlarm   = "runtime.alarm.execute"
	KindTerminateActor = "runtime.actor.terminate"
	KindDisconnect     = "runtime.disconnect"

	// Runtime -> Host responses

	KindExecuteAlarmResponse   = "runtime.alarm.execute.response"
	KindTerminateActorResponse = "runtime.actor.terminate.response"
	KindDisconnectResponse     = "runtime.disconnect.response"

	// Host -> Host

	KindInvokeActor         = "invoke.actor"
	KindInvokeActorResponse = "invoke.actor.response"

	// Error response, correlated to a request via CorrelationID; carries a serialized Error payload

	KindError = "error"
)

// Envelope is the shared wrapper for every protocol message
// It is encoded with MessagePack and framed with a length prefix on a stream
type Envelope struct {
	// ProtocolVersion is the version of the protocol used to encode this message
	ProtocolVersion uint16 `msgpack:"v"`
	// MessageID uniquely identifies this message and is used to correlate responses
	MessageID string `msgpack:"id"`
	// CorrelationID is set on a response to the MessageID of the request it answers
	CorrelationID string `msgpack:"cid,omitempty"`
	// Kind identifies the message type
	Kind string `msgpack:"k"`
	// HostID is the stable identity of the host sending or targeted by the message
	HostID string `msgpack:"h,omitempty"`
	// SessionID identifies the runtime session, used to detect superseded sessions
	SessionID string `msgpack:"s,omitempty"`
	// Payload is the MessagePack-encoded, kind-specific body
	Payload []byte `msgpack:"p,omitempty"`
}

// NewEnvelope returns a new request envelope with a freshly-generated MessageID
func NewEnvelope(kind string, payload []byte) *Envelope {
	return &Envelope{
		ProtocolVersion: ProtocolVersion,
		MessageID:       uuid.NewString(),
		Kind:            kind,
		Payload:         payload,
	}
}

// NewRequest returns a new request envelope with the given payload object encoded as MessagePack
func NewRequest(kind string, payload any) (*Envelope, error) {
	e := NewEnvelope(kind, nil)
	err := e.SetPayload(payload)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// Reply returns a new response envelope correlated to this request
func (e *Envelope) Reply(kind string, payload []byte) *Envelope {
	r := NewEnvelope(kind, payload)
	r.CorrelationID = e.MessageID
	return r
}

// ReplyWith returns a response envelope of the given kind correlated to this request, encoding payload as MessagePack
// A nil payload produces an empty-bodied acknowledgement, which is how ack-only operations respond
func (e *Envelope) ReplyWith(kind string, payload any) (*Envelope, error) {
	r := e.Reply(kind, nil)
	err := r.SetPayload(payload)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// ErrorReply returns a KindError response correlated to this request, carrying the structured error
func (e *Envelope) ErrorReply(perr *Error) *Envelope {
	// Marshal directly: if it somehow fails we still return a usable error envelope
	payload, err := Marshal(perr)
	if err != nil {
		payload, _ = Marshal(NewError(ErrCodeInternal, "failed to encode error"))
	}
	return e.Reply(KindError, payload)
}

// SetPayload encodes v as MessagePack and stores it as the envelope payload
// A nil value clears the payload
func (e *Envelope) SetPayload(v any) error {
	if v == nil {
		e.Payload = nil
		return nil
	}

	b, err := Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to encode payload: %w", err)
	}
	if len(b) > MaxPayloadSize {
		return fmt.Errorf("payload size %d exceeds maximum %d", len(b), MaxPayloadSize)
	}

	e.Payload = b
	return nil
}

// DecodePayload decodes the envelope payload into v
// An empty payload is a no-op
func (e *Envelope) DecodePayload(v any) error {
	if len(e.Payload) == 0 {
		return nil
	}
	return Unmarshal(e.Payload, v)
}

// AsError returns the structured error carried by a KindError envelope
// The second return value is false if this envelope is not an error
func (e *Envelope) AsError() (*Error, bool) {
	if e.Kind != KindError {
		return nil, false
	}

	perr := &Error{}
	err := e.DecodePayload(perr)
	if err != nil {
		return NewError(ErrCodeInternal, "failed to decode error payload"), true
	}
	return perr, true
}

// CheckProtocolVersion validates the protocol version advertised by a peer
// A higher version than this peer supports is rejected; equal or lower is accepted and the lower is used
func CheckProtocolVersion(v uint16) error {
	if v == 0 {
		return errors.New("missing protocol version")
	}
	if v > ProtocolVersion {
		return fmt.Errorf("unsupported protocol version %d: this peer supports up to %d", v, ProtocolVersion)
	}
	return nil
}

// NegotiateVersion returns the version both peers should use, given the peer's advertised version
func NegotiateVersion(peerVersion uint16) uint16 {
	if peerVersion == 0 || peerVersion > ProtocolVersion {
		return ProtocolVersion
	}
	return peerVersion
}

// Marshal encodes v using MessagePack
func Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

// Unmarshal decodes MessagePack-encoded data into v
func Unmarshal(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

// WriteMessage writes a length-prefixed, MessagePack-encoded envelope to w
// The frame is a big-endian uint32 length followed by the encoded envelope
func WriteMessage(w io.Writer, e *Envelope) error {
	b, err := Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to encode envelope: %w", err)
	}
	if len(b) > MaxMessageSize {
		return fmt.Errorf("message size %d exceeds maximum %d", len(b), MaxMessageSize)
	}

	var lenBuf [4]byte
	// len(b) is bounded by MaxMessageSize above, well within uint32
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b))) //nolint:gosec

	_, err = w.Write(lenBuf[:])
	if err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	_, err = w.Write(b)
	if err != nil {
		return fmt.Errorf("failed to write message body: %w", err)
	}
	return nil
}

// ReadMessage reads a single length-prefixed envelope from r
// It returns io.EOF if the stream is closed cleanly before any byte is read
func ReadMessage(r io.Reader) (*Envelope, error) {
	var lenBuf [4]byte
	_, err := io.ReadFull(r, lenBuf[:])
	if err != nil {
		return nil, err
	}

	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > MaxMessageSize {
		return nil, fmt.Errorf("incoming message size %d exceeds maximum %d", n, MaxMessageSize)
	}

	buf := make([]byte, n)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read message body: %w", err)
	}

	e := &Envelope{}
	err = Unmarshal(buf, e)
	if err != nil {
		return nil, fmt.Errorf("failed to decode envelope: %w", err)
	}
	return e, nil
}
