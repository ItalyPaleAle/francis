package host

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

// objectEnvelope implements actor.Envelope to return an object that was not serialized
type objectEnvelope struct {
	object any
}

func newObjectEnvelope(obj any) *objectEnvelope {
	return &objectEnvelope{
		object: obj,
	}
}

func (o *objectEnvelope) Decode(into any) error {
	if o == nil || o.object == nil {
		return nil
	}

	if into == nil {
		return errors.New("target object is nil")
	}

	intoVal := reflect.ValueOf(into)
	if intoVal.Kind() != reflect.Pointer || intoVal.IsNil() {
		return errors.New("parameter out must be a non-nil pointer")
	}

	objVal := reflect.ValueOf(o.object)
	if objVal.IsZero() {
		// Object is zero value
		return nil
	}

	// Fast path, try to assign the object directly
	if objVal.Type().AssignableTo(intoVal.Elem().Type()) {
		intoVal.Elem().Set(objVal)
		return nil
	}

	// Serialize the data using msgpack and deserialize it into the target
	buf := bytes.Buffer{}
	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)
	enc.Reset(&buf)
	err := enc.Encode(o.object)
	if err != nil {
		return fmt.Errorf("failed to serialize data using msgpack: %w", err)
	}

	dec := msgpack.GetDecoder()
	defer msgpack.PutDecoder(dec)
	dec.Reset(&buf)
	err = dec.Decode(into)
	if err != nil {
		return fmt.Errorf("failed to deserialize data using msgpack: %w", err)
	}

	return nil
}

func (o *objectEnvelope) Encode(w io.Writer) error {
	if o == nil || o.object == nil {
		return nil
	}

	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)

	enc.Reset(w)
	err := enc.Encode(o.object)
	if err != nil {
		return fmt.Errorf("failed to serialize data using msgpack: %w", err)
	}

	return nil
}
