package backup

import (
	"bytes"
	"io"
	"iter"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// collect drains a record iterator into a slice, failing on any decode error
func collect(t *testing.T, seq iter.Seq2[Record, error]) []Record {
	t.Helper()

	var recs []Record
	for rec, err := range seq {
		require.NoError(t, err)
		recs = append(recs, rec)
	}

	return recs
}

func TestRoundTrip(t *testing.T) {
	createdAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	exp := time.Date(2026, 7, 2, 8, 30, 0, 0, time.UTC)

	state := &StateRecord{
		ActorType:  "Counter",
		ActorID:    "c-1",
		Data:       []byte{0x01, 0x02, 0x03},
		Expiration: &exp,
	}
	alarm := &AlarmRecord{
		ID:        "0198d0aa-0000-7000-8000-000000000001",
		ActorType: "Counter",
		ActorID:   "c-1",
		Name:      "tick",
		DueTime:   time.Date(2026, 7, 1, 13, 0, 0, 0, time.UTC),
		Interval:  "PT1M",
		Data:      []byte("payload"),
		Kind:      "alarm",
	}
	job := &AlarmRecord{
		ID:        "0198d0aa-0000-7000-8000-000000000002",
		ActorType: "Worker",
		ActorID:   "w-9",
		Name:      "send",
		DueTime:   time.Date(2026, 7, 1, 14, 0, 0, 0, time.UTC),
		Kind:      "job",
		JobMethod: "Send",
	}
	dead := &DeadJobRecord{
		JobID:       "0198d0aa-0000-7000-8000-000000000003",
		ActorType:   "Worker",
		ActorID:     "w-9",
		Method:      "Send",
		Data:        []byte("dead-payload"),
		Attempts:    5,
		LastError:   "boom",
		FailedAt:    time.Date(2026, 7, 1, 15, 0, 0, 0, time.UTC),
		OriginalDue: time.Date(2026, 7, 1, 14, 0, 0, 0, time.UTC),
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, createdAt)
	require.NoError(t, err)
	require.NoError(t, w.WriteState(state))
	require.NoError(t, w.WriteAlarm(alarm))
	require.NoError(t, w.WriteAlarm(job))
	require.NoError(t, w.WriteDeadJob(dead))

	r, hdr, err := NewReader(&buf)
	require.NoError(t, err)
	assert.Equal(t, Format, hdr.Format)
	assert.Equal(t, Version, hdr.Version)
	assert.True(t, createdAt.Equal(hdr.CreatedAt), "createdAt mismatch: got %v", hdr.CreatedAt)

	recs := collect(t, r.All())
	require.Len(t, recs, 4)

	require.Equal(t, RecordTypeState, recs[0].Type)
	require.NotNil(t, recs[0].State)
	assert.Equal(t, state.ActorType, recs[0].State.ActorType)
	assert.Equal(t, state.ActorID, recs[0].State.ActorID)
	assert.Equal(t, state.Data, recs[0].State.Data)
	require.NotNil(t, recs[0].State.Expiration)
	assert.True(t, exp.Equal(*recs[0].State.Expiration))

	require.Equal(t, RecordTypeAlarm, recs[1].Type)
	require.NotNil(t, recs[1].Alarm)
	assert.Equal(t, alarm.ID, recs[1].Alarm.ID)
	assert.Equal(t, alarm.Interval, recs[1].Alarm.Interval)
	assert.Equal(t, alarm.Data, recs[1].Alarm.Data)
	assert.True(t, alarm.DueTime.Equal(recs[1].Alarm.DueTime))

	require.NotNil(t, recs[2].Alarm)
	assert.Equal(t, "job", recs[2].Alarm.Kind)
	assert.Equal(t, "Send", recs[2].Alarm.JobMethod)

	require.Equal(t, RecordTypeDeadJob, recs[3].Type)
	require.NotNil(t, recs[3].DeadJob)
	assert.Equal(t, dead.JobID, recs[3].DeadJob.JobID)
	assert.Equal(t, dead.Attempts, recs[3].DeadJob.Attempts)
	assert.Equal(t, dead.LastError, recs[3].DeadJob.LastError)
	assert.Equal(t, dead.Data, recs[3].DeadJob.Data)
	assert.True(t, dead.FailedAt.Equal(recs[3].DeadJob.FailedAt))
	assert.True(t, dead.OriginalDue.Equal(recs[3].DeadJob.OriginalDue))
}

func TestEmptyBackupHasHeaderOnly(t *testing.T) {
	var buf bytes.Buffer
	_, err := NewWriter(&buf, time.Now())
	require.NoError(t, err)

	r, hdr, err := NewReader(&buf)
	require.NoError(t, err)
	assert.Equal(t, Version, hdr.Version)

	recs := collect(t, r.All())
	assert.Empty(t, recs)
}

// TestPullSectionBoundary exercises the iter.Pull2 access pattern the Postgres restore relies on: pull records one at a time and stop at a section boundary
func TestPullSectionBoundary(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, time.Now())
	require.NoError(t, err)
	require.NoError(t, w.WriteState(&StateRecord{ActorType: "A", ActorID: "1"}))
	require.NoError(t, w.WriteState(&StateRecord{ActorType: "A", ActorID: "2"}))
	require.NoError(t, w.WriteAlarm(&AlarmRecord{ID: "x", ActorType: "A", ActorID: "1", Name: "n"}))

	r, _, err := NewReader(&buf)
	require.NoError(t, err)

	next, stop := iter.Pull2(r.All())
	defer stop()

	// Consume the state section, stopping at the first non-state record
	var (
		states   int
		boundary Record
	)
	for {
		rec, err, ok := next()
		require.NoError(t, err)
		require.True(t, ok)
		if rec.Type != RecordTypeState {
			boundary = rec
			break
		}
		states++
	}
	assert.Equal(t, 2, states)

	// The record that ended the section is the alarm, and the stream is then exhausted
	assert.Equal(t, RecordTypeAlarm, boundary.Type)
	assert.Equal(t, "x", boundary.Alarm.ID)

	_, _, ok := next()
	assert.False(t, ok)
}

func TestDecodeErrorPropagates(t *testing.T) {
	var buf bytes.Buffer
	_, err := NewWriter(&buf, time.Now())
	require.NoError(t, err)
	// 0xc1 is the MessagePack "never used" byte, so decoding the next value fails
	buf.WriteByte(0xc1)

	r, _, err := NewReader(&buf)
	require.NoError(t, err)

	var gotErr error
	for _, err := range r.All() {
		if err != nil {
			gotErr = err
			break
		}
	}
	require.Error(t, gotErr)
}

func TestEmptyStreamIsBadFormat(t *testing.T) {
	_, _, err := NewReader(bytes.NewReader(nil))
	require.ErrorIs(t, err, ErrBadFormat)
}

func TestWrongFormatRejected(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	err := enc.Encode(&Header{Format: "something-else", Version: 1})
	require.NoError(t, err)

	_, _, err = NewReader(&buf)
	require.ErrorIs(t, err, ErrBadFormat)
}

func TestNewerVersionRejected(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	err := enc.Encode(&Header{Format: Format, Version: Version + 1})
	require.NoError(t, err)

	_, _, err = NewReader(&buf)
	require.ErrorIs(t, err, ErrUnsupportedVersion)
}

func TestEmptyDataNormalizedToNil(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, time.Now())
	require.NoError(t, err)
	err = w.WriteState(&StateRecord{ActorType: "A", ActorID: "1", Data: []byte{}})
	require.NoError(t, err)

	r, _, err := NewReader(&buf)
	require.NoError(t, err)
	recs := collect(t, r.All())
	require.Len(t, recs, 1)
	assert.Nil(t, recs[0].State.Data)
}

func TestSameStreamHasSingleHeader(t *testing.T) {
	// Guards against accidentally writing more than one header value
	var buf bytes.Buffer
	w, err := NewWriter(&buf, time.Now())
	require.NoError(t, err)
	err = w.WriteState(&StateRecord{ActorType: "A", ActorID: "1"})
	require.NoError(t, err)

	dec := msgpack.NewDecoder(&buf)
	var hdr Header
	require.NoError(t, dec.Decode(&hdr))
	require.Equal(t, Format, hdr.Format)

	var rec Record
	require.NoError(t, dec.Decode(&rec))
	assert.Equal(t, RecordTypeState, rec.Type)

	err = dec.Decode(&Record{})
	require.ErrorIs(t, err, io.EOF)
}
