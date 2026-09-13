// Package backup defines a portable, versioned, streaming format for exporting and importing the persistent data of a Francis actor provider: actor state, alarms (including live jobs), and terminal jobs
//
// The format is provider-neutral: timestamps are carried as time.Time and binary payloads as byte slices, so a backup taken from one provider (for example PostgreSQL) can be restored into a different one (for example SQLite)
// It is encoded with MessagePack and handled as a stream, so arbitrarily large datasets can be read and written without being buffered in memory
//
// The wire layout is a single Header value followed by a sequence of Record values
// Records are emitted grouped by type in a fixed order (state, then alarms, then terminal jobs), which lets a reader load one entity type at a time (and, for providers that support it, bulk-load each section) while relying on Reader.Unread to detect a section boundary with a single record of lookahead
package backup

import (
	"errors"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	// Format identifies a Francis backup stream
	// It is written into the header and validated on read so an unrelated stream is rejected early
	Format = "francis-backup"

	// Version is the current backup format version
	// It is written into the header, and a reader rejects a stream whose version is newer than it supports
	Version = 1
)

var (
	// ErrBadFormat is returned when a stream is not a Francis backup
	ErrBadFormat = errors.New("not a valid francis backup stream")

	// ErrUnsupportedVersion is returned when a stream was produced by a newer, incompatible version of the backup format
	ErrUnsupportedVersion = errors.New("unsupported backup format version")
)

// RecordType discriminates the payload carried by a Record
type RecordType string

const (
	RecordTypeState       RecordType = "state"
	RecordTypeAlarm       RecordType = "alarm"
	RecordTypeTerminalJob RecordType = "terminaljob"
)

// Header is the first value in a backup stream
type Header struct {
	Format    string    `msgpack:"format"`
	Version   int       `msgpack:"version"`
	CreatedAt time.Time `msgpack:"createdAt"`
}

// StateRecord is a single actor-state entry
// Expiration is nil when the state does not expire
type StateRecord struct {
	ActorType  string            `msgpack:"actorType"`
	ActorID    string            `msgpack:"actorId"`
	Data       []byte            `msgpack:"data,omitempty"`
	Expiration *time.Time        `msgpack:"expiration,omitempty"`
	Labels     map[string]string `msgpack:"labels,omitempty"`
}

// AlarmRecord is a single alarm (Kind "alarm") or live job (Kind "job")
// The ephemeral lease fields are intentionally not part of the format
type AlarmRecord struct {
	ID        string     `msgpack:"id"`
	ActorType string     `msgpack:"actorType"`
	ActorID   string     `msgpack:"actorId"`
	Name      string     `msgpack:"name"`
	DueTime   time.Time  `msgpack:"dueTime"`
	Interval  string     `msgpack:"interval,omitempty"`
	Cron      string     `msgpack:"cron,omitempty"`
	TTL       *time.Time `msgpack:"ttl,omitempty"`
	Data      []byte     `msgpack:"data,omitempty"`
	Kind      string     `msgpack:"kind,omitempty"`
	JobMethod string     `msgpack:"jobMethod,omitempty"`
}

// TerminalJobRecord is a single job that ended, either by completing or by dead-lettering
// Expiration is nil when the record does not expire
type TerminalJobRecord struct {
	JobID       string     `msgpack:"jobId"`
	ActorType   string     `msgpack:"actorType"`
	ActorID     string     `msgpack:"actorId"`
	Method      string     `msgpack:"method"`
	Data        []byte     `msgpack:"data,omitempty"`
	Status      string     `msgpack:"status"`
	Attempts    int        `msgpack:"attempts"`
	LastError   string     `msgpack:"lastError,omitempty"`
	EndedAt     time.Time  `msgpack:"endedAt"`
	OriginalDue time.Time  `msgpack:"originalDue"`
	Interval    string     `msgpack:"interval,omitempty"`
	Cron        string     `msgpack:"cron,omitempty"`
	Expiration  *time.Time `msgpack:"expiration,omitempty"`
}

// Record is one entry in a backup stream
// Exactly one payload pointer is set, selected by Type
type Record struct {
	Type        RecordType         `msgpack:"type"`
	State       *StateRecord       `msgpack:"state,omitempty"`
	Alarm       *AlarmRecord       `msgpack:"alarm,omitempty"`
	TerminalJob *TerminalJobRecord `msgpack:"terminalJob,omitempty"`
}

// Writer streams backup records to an io.Writer
// The header is written when the Writer is created, and callers then write records grouped by type in the order state, alarms, terminal jobs
type Writer struct {
	enc *msgpack.Encoder
}

// NewWriter writes the backup header to w and returns a Writer for the records
func NewWriter(w io.Writer, createdAt time.Time) (*Writer, error) {
	enc := msgpack.NewEncoder(w)
	hdr := Header{
		Format:    Format,
		Version:   Version,
		CreatedAt: createdAt.UTC(),
	}

	err := enc.Encode(&hdr)
	if err != nil {
		return nil, fmt.Errorf("failed to write backup header: %w", err)
	}

	return &Writer{enc: enc}, nil
}

// WriteState writes an actor-state record
func (w *Writer) WriteState(r *StateRecord) error {
	return w.write(Record{Type: RecordTypeState, State: r})
}

// WriteAlarm writes an alarm record
func (w *Writer) WriteAlarm(r *AlarmRecord) error {
	return w.write(Record{Type: RecordTypeAlarm, Alarm: r})
}

// WriteTerminalJob writes a terminal-job record
func (w *Writer) WriteTerminalJob(r *TerminalJobRecord) error {
	return w.write(Record{Type: RecordTypeTerminalJob, TerminalJob: r})
}

func (w *Writer) write(rec Record) error {
	err := w.enc.Encode(&rec)
	if err != nil {
		return fmt.Errorf("failed to write backup record: %w", err)
	}
	return nil
}

// Reader streams backup records from an io.Reader
type Reader struct {
	dec *msgpack.Decoder
}

// NewReader reads and validates the backup header from r and returns a Reader for the records along with the decoded Header
// It returns ErrBadFormat if the stream is not a Francis backup, or ErrUnsupportedVersion if it was produced by a newer format version
func NewReader(r io.Reader) (*Reader, Header, error) {
	dec := msgpack.NewDecoder(r)

	// Decode the header, which is always the first value in the stream
	var hdr Header
	err := dec.Decode(&hdr)
	if errors.Is(err, io.EOF) {
		return nil, Header{}, fmt.Errorf("%w: stream is empty", ErrBadFormat)
	} else if err != nil {
		return nil, Header{}, fmt.Errorf("failed to read backup header: %w", err)
	}

	// Reject anything that is not a backup we can read
	if hdr.Format != Format {
		return nil, Header{}, fmt.Errorf("%w: unexpected format %q", ErrBadFormat, hdr.Format)
	}
	if hdr.Version > Version {
		return nil, Header{}, fmt.Errorf("%w: stream is version %d, this build supports up to %d", ErrUnsupportedVersion, hdr.Version, Version)
	}

	return &Reader{dec: dec}, hdr, nil
}

// All returns an iterator over the records in the stream, in the order they were written
// A decode error is yielded once with a zero-value record, after which iteration ends
// Records are grouped by type in a fixed order (state, then alarms, then dead jobs), so a consumer that needs to process one section at a time can pair this with iter.Pull2 and stop when the record type changes
func (r *Reader) All() iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		for {
			// Decode the next value, ending cleanly at the end of the stream
			var rec Record
			err := r.dec.Decode(&rec)
			if errors.Is(err, io.EOF) {
				return
			} else if err != nil {
				yield(Record{}, err)
				return
			}

			normalizeRecord(&rec)

			if !yield(rec, nil) {
				return
			}
		}
	}
}

// normalizeRecord canonicalizes a decoded record so an empty byte slice is represented as nil, matching the providers' len(data)==0 convention
func normalizeRecord(rec *Record) {
	switch {
	case rec.State != nil:
		if len(rec.State.Data) == 0 {
			rec.State.Data = nil
		}
	case rec.Alarm != nil:
		if len(rec.Alarm.Data) == 0 {
			rec.Alarm.Data = nil
		}
	case rec.TerminalJob != nil:
		if len(rec.TerminalJob.Data) == 0 {
			rec.TerminalJob.Data = nil
		}
	}
}
