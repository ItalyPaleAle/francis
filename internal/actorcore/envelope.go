package actorcore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

var timeType = reflect.TypeFor[time.Time]()

// ObjectEnvelope implements actor.Envelope to carry an object that was not serialized
type ObjectEnvelope struct {
	object any
}

// NewObjectEnvelope returns an ObjectEnvelope wrapping obj
func NewObjectEnvelope(obj any) *ObjectEnvelope {
	return &ObjectEnvelope{
		object: obj,
	}
}

// Decode decodes the wrapped object into into
func (o *ObjectEnvelope) Decode(into any) error {
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

	// Fast path for assignable types: copy pure value types directly and deep-copy composite reference types so caller and actor never share backing memory
	if objVal.Type().AssignableTo(intoVal.Elem().Type()) {
		if !containsMutableRef(objVal.Type()) {
			intoVal.Elem().Set(objVal)
			return nil
		}
		copied, ok := deepCopyValue(objVal, nil)
		if ok {
			intoVal.Elem().Set(copied)
			return nil
		}
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

// Encode writes the wrapped object to w as MessagePack
func (o *ObjectEnvelope) Encode(w io.Writer) error {
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

// containsMutableRef reports whether values of type t can share mutable backing storage when copied by shallow assignment
func containsMutableRef(t reflect.Type) bool {
	if t == timeType {
		return false
	}

	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.String:
		return false
	case reflect.Array:
		return containsMutableRef(t.Elem())
	case reflect.Struct:
		for f := range t.Fields() {
			if containsMutableRef(f.Type) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

type ptrVisit struct {
	addr uintptr
	typ  reflect.Type
}

// deepCopyValue produces a deep copy of v so slices, maps, and pointers do not alias across local actor invocations
// It returns false if v contains an unexported reference field or unsupported runtime handle, letting Decode fall back to MessagePack
func deepCopyValue(v reflect.Value, visited map[ptrVisit]reflect.Value) (reflect.Value, bool) {
	if !v.IsValid() {
		return v, true
	}
	if !containsMutableRef(v.Type()) {
		return v, true
	}

	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type()), true
		}

		elemCopy, ok := deepCopyValue(v.Elem(), visited)
		if !ok {
			return reflect.Value{}, false
		}

		out := reflect.New(v.Type()).Elem()
		out.Set(elemCopy)
		return out, true

	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type()), true
		}

		key := ptrVisit{
			addr: v.Pointer(),
			typ:  v.Type(),
		}
		prev, seen := visited[key]
		if seen {
			return prev, true
		}

		if visited == nil {
			visited = make(map[ptrVisit]reflect.Value)
		}
		out := reflect.New(v.Type().Elem())
		visited[key] = out
		elemCopy, ok := deepCopyValue(v.Elem(), visited)
		if !ok {
			return reflect.Value{}, false
		}

		out.Elem().Set(elemCopy)
		return out, true

	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type()), true
		}

		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		if !containsMutableRef(v.Type().Elem()) {
			reflect.Copy(out, v)
			return out, true
		}

		for i := range v.Len() {
			elemCopy, ok := deepCopyValue(v.Index(i), visited)
			if !ok {
				return reflect.Value{}, false
			}

			out.Index(i).Set(elemCopy)
		}

		return out, true

	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type()), true
		}

		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			kCopy, ok := deepCopyValue(iter.Key(), visited)
			if !ok {
				return reflect.Value{}, false
			}

			vCopy, ok := deepCopyValue(iter.Value(), visited)
			if !ok {
				return reflect.Value{}, false
			}

			out.SetMapIndex(kCopy, vCopy)
		}

		return out, true

	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		out.Set(v)
		for i := range v.NumField() {
			if !containsMutableRef(v.Type().Field(i).Type) {
				continue
			}

			dstField := out.Field(i)
			if !dstField.CanSet() {
				return reflect.Value{}, false
			}

			fieldCopy, ok := deepCopyValue(v.Field(i), visited)
			if !ok {
				return reflect.Value{}, false
			}

			dstField.Set(fieldCopy)
		}
		return out, true

	case reflect.Array:
		out := reflect.New(v.Type()).Elem()
		for i := range v.Len() {
			elemCopy, ok := deepCopyValue(v.Index(i), visited)
			if !ok {
				return reflect.Value{}, false
			}

			out.Index(i).Set(elemCopy)
		}

		return out, true

	default:
		return reflect.Value{}, false
	}
}
