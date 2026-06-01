package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvelopePayloadRoundTrip(t *testing.T) {
	in := RegisterHostRequest{
		ProtocolVersion: ProtocolVersion,
		Address:         "10.0.0.5:8443",
		ActorTypes: []ActorHostType{
			{ActorType: "myactor", IdleTimeoutMs: 10000, SupportsObjectInvocation: true},
		},
	}

	e := NewEnvelope(KindRegisterHost, nil)
	require.NoError(t, e.SetPayload(in))
	require.NotEmpty(t, e.MessageID)
	require.Equal(t, ProtocolVersion, e.ProtocolVersion)

	var out RegisterHostRequest
	require.NoError(t, e.DecodePayload(&out))
	assert.Equal(t, in, out)
}

func TestEnvelopeNilPayload(t *testing.T) {
	e := NewEnvelope(KindHealthCheck, nil)
	require.NoError(t, e.SetPayload(nil))
	assert.Nil(t, e.Payload)

	// Decoding an empty payload is a no-op and must not error
	var out HealthCheckRequest
	require.NoError(t, e.DecodePayload(&out))
}

func TestEnvelopeReplyCorrelation(t *testing.T) {
	req := NewEnvelope(KindLookupActor, nil)

	res, err := req.ReplyWith(KindLookupActorResponse, LookupActorResponse{HostID: "h1", Address: "1.2.3.4:443"})
	require.NoError(t, err)
	assert.Equal(t, req.MessageID, res.CorrelationID)
	assert.Equal(t, KindLookupActorResponse, res.Kind)
	assert.NotEqual(t, req.MessageID, res.MessageID)

	var out LookupActorResponse
	require.NoError(t, res.DecodePayload(&out))
	assert.Equal(t, "h1", out.HostID)
}

func TestWriteReadMessageRoundTrip(t *testing.T) {
	e := NewEnvelope(KindSetState, nil)
	e.HostID = "host-abc"
	e.SessionID = "sess-1"
	require.NoError(t, e.SetPayload(SetStateRequest{
		ActorRef: ActorRef{ActorType: "t", ActorID: "id"},
		Data:     []byte("state-bytes"),
	}))

	var buf bytes.Buffer
	require.NoError(t, WriteMessage(&buf, e))

	got, err := ReadMessage(&buf)
	require.NoError(t, err)
	assert.Equal(t, e.MessageID, got.MessageID)
	assert.Equal(t, e.HostID, got.HostID)
	assert.Equal(t, e.SessionID, got.SessionID)
	assert.Equal(t, KindSetState, got.Kind)

	var out SetStateRequest
	require.NoError(t, got.DecodePayload(&out))
	assert.Equal(t, "t", out.ActorType)
	assert.Equal(t, []byte("state-bytes"), out.Data)
}

func TestWriteReadMultipleMessages(t *testing.T) {
	var buf bytes.Buffer
	kinds := []string{KindGetState, KindSetState, KindDeleteState}
	for _, k := range kinds {
		require.NoError(t, WriteMessage(&buf, NewEnvelope(k, nil)))
	}

	for _, k := range kinds {
		got, err := ReadMessage(&buf)
		require.NoError(t, err)
		assert.Equal(t, k, got.Kind)
	}

	// The stream is now drained, so the next read reports EOF
	_, err := ReadMessage(&buf)
	assert.ErrorIs(t, err, io.EOF)
}

func TestReadMessageRejectsOversizedFrame(t *testing.T) {
	var buf bytes.Buffer
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], MaxMessageSize+1)
	buf.Write(lenBuf[:])

	_, err := ReadMessage(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

func TestSetPayloadRejectsOversizedPayload(t *testing.T) {
	e := NewEnvelope(KindSetState, nil)
	big := SetStateRequest{Data: make([]byte, MaxPayloadSize+1)}

	err := e.SetPayload(big)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

func TestCheckProtocolVersion(t *testing.T) {
	require.NoError(t, CheckProtocolVersion(ProtocolVersion))

	require.Error(t, CheckProtocolVersion(0))
	require.Error(t, CheckProtocolVersion(ProtocolVersion+1))
}

func TestNegotiateVersion(t *testing.T) {
	assert.Equal(t, ProtocolVersion, NegotiateVersion(0))
	assert.Equal(t, ProtocolVersion, NegotiateVersion(ProtocolVersion+1))
	assert.Equal(t, ProtocolVersion, NegotiateVersion(ProtocolVersion))
}

func TestErrorIsByCode(t *testing.T) {
	err := NewErrorf(ErrCodeRetryLater, "actor moving from %s", "host-1")

	// errors.Is matches on the code, not the message
	require.ErrorIs(t, err, NewError(ErrCodeRetryLater, ""))
	require.NotErrorIs(t, err, NewError(ErrCodeInternal, ""))
}

func TestErrorRetryable(t *testing.T) {
	assert.True(t, NewError(ErrCodeRetryLater, "").Retryable())
	assert.True(t, NewError(ErrCodeHostDraining, "").Retryable())
	assert.True(t, NewError(ErrCodeActorHalted, "").Retryable())
	assert.True(t, NewError(ErrCodeHostMismatch, "").Retryable())
	assert.False(t, NewError(ErrCodeInternal, "").Retryable())
	assert.False(t, NewError(ErrCodeStateNotFound, "").Retryable())
}

func TestErrorRetryAfterAndMetadata(t *testing.T) {
	err := NewError(ErrCodeHostDraining, "draining").
		WithRetryAfter(250 * time.Millisecond).
		WithMetadata(map[string]string{"foo": "bar"})

	d, ok := err.RetryAfter()
	require.True(t, ok)
	assert.Equal(t, 250*time.Millisecond, d)
	assert.Equal(t, "bar", err.Metadata["foo"])
}

func TestErrorReplyAndAsError(t *testing.T) {
	req := NewEnvelope(KindInvokeActor, nil)
	perr := NewError(ErrCodeActorHalted, "halted").WithRetryAfter(time.Second)

	res := req.ErrorReply(perr)
	assert.Equal(t, KindError, res.Kind)
	assert.Equal(t, req.MessageID, res.CorrelationID)

	got, ok := res.AsError()
	require.True(t, ok)
	assert.Equal(t, ErrCodeActorHalted, got.Code)
	assert.Equal(t, "halted", got.Message)
	assert.Equal(t, int64(1000), got.RetryAfterMs)

	// A non-error envelope reports false
	_, ok = req.AsError()
	assert.False(t, ok)
}

func TestErrorAsErrorWrappedChain(t *testing.T) {
	// A wrapped protocol error still matches by code through errors.As
	wrapped := errors.Join(errors.New("context"), NewError(ErrCodeNoHost, "no host"))
	assert.ErrorIs(t, wrapped, NewError(ErrCodeNoHost, ""))
}

func TestAlarmDTORoundTrip(t *testing.T) {
	in := SetAlarmRequest{
		AlarmRef:        AlarmRef{ActorType: "t", ActorID: "id", Name: "wake"},
		AlarmProperties: AlarmProperties{DueTimeUnixMs: 123456, Interval: "PT1M", Data: []byte("d")},
	}

	b, err := Marshal(in)
	require.NoError(t, err)

	var out SetAlarmRequest
	require.NoError(t, Unmarshal(b, &out))
	assert.Equal(t, in, out)
}

func TestExecuteAlarmRoundTrip(t *testing.T) {
	in := ExecuteAlarmRequest{
		ActorType:     "t",
		ActorID:       "id",
		Name:          "wake",
		AlarmID:       "alarm-1",
		DueTimeUnixMs: 999,
		Attempts:      2,
		Data:          []byte("payload"),
	}

	e := NewEnvelope(KindExecuteAlarm, nil)
	require.NoError(t, e.SetPayload(in))

	var out ExecuteAlarmRequest
	require.NoError(t, e.DecodePayload(&out))
	assert.Equal(t, in, out)
}

func TestInvokeActorRoundTrip(t *testing.T) {
	in := InvokeActorRequest{
		TargetHostID: "host-9",
		ActorType:    "t",
		ActorID:      "id",
		Method:       "DoThing",
		Mode:         InvocationModeStream,
		ContentType:  "application/octet-stream",
	}

	b, err := Marshal(in)
	require.NoError(t, err)

	var out InvokeActorRequest
	require.NoError(t, Unmarshal(b, &out))
	assert.Equal(t, in, out)
}
