// Package msgpackjson converts MessagePack values to JSON, so values stored as MessagePack can be shown to people and to JSON clients
// Every number is rendered as a JSON string, so integers keep their exact value and floats keep their shortest representation in any JSON parser: integers as decimal strings, finite floats as their shortest representation, and NaN and infinities as "NaN", "Infinity", and "-Infinity"
// Values without a JSON equivalent use a fixed convention: timestamps become RFC 3339 strings, binary data and strings that aren't valid UTF-8 become {"$binary":"<base64>"}, other extensions become {"$ext":<type>,"data":"<base64>"}, and map keys that aren't strings become their text
package msgpackjson

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// maxDepth bounds the nesting of converted values, so a hostile value cannot exhaust the stack
const maxDepth = 512

// timestampExtType is the MessagePack extension type of timestamps
const timestampExtType = -1

// ToJSON converts a single MessagePack value to JSON, rendering numbers and values without a JSON equivalent with the package convention
// lossy is true when any value used a rendering that changes its shape: binary data, an extension, a timestamp, a string that isn't valid UTF-8, or a map key that isn't a string
// Numbers rendered as strings don't make the result lossy, since that is the convention for every number
func ToJSON(data []byte) (out json.RawMessage, lossy bool, err error) {
	r := bytes.NewReader(data)
	c := &converter{
		r:   r,
		dec: msgpack.NewDecoder(r),
	}
	err = c.convert(0)
	if err != nil {
		return nil, false, err
	}

	// The stored value must be a single MessagePack value
	_, err = c.dec.PeekCode()
	if err == nil {
		return nil, false, errors.New("trailing data after the MessagePack value")
	} else if !errors.Is(err, io.EOF) {
		return nil, false, err
	}

	return c.buf.Bytes(), c.lossy, nil
}

type converter struct {
	// r is the input the decoder reads from, which tells how many bytes remain
	r     *bytes.Reader
	dec   *msgpack.Decoder
	buf   bytes.Buffer
	lossy bool
}

func (c *converter) convert(depth int) error {
	if depth > maxDepth {
		return errors.New("MessagePack value is nested too deeply")
	}

	code, err := c.dec.PeekCode()
	if err != nil {
		return err
	}

	switch {
	case code == msgpcode.Nil:
		err = c.dec.DecodeNil()
		if err != nil {
			return err
		}
		c.buf.WriteString("null")

	case code == msgpcode.True || code == msgpcode.False:
		v, err := c.dec.DecodeBool()
		if err != nil {
			return err
		}
		c.buf.WriteString(strconv.FormatBool(v))

	// A uint64 is decoded on its own because it can exceed the range of an int64
	case code == msgpcode.Uint64:
		v, err := c.dec.DecodeUint64()
		if err != nil {
			return err
		}
		c.writeString(strconv.FormatUint(v, 10))

	case msgpcode.IsFixedNum(code),
		code == msgpcode.Uint8, code == msgpcode.Uint16, code == msgpcode.Uint32,
		code == msgpcode.Int8, code == msgpcode.Int16, code == msgpcode.Int32, code == msgpcode.Int64:
		v, err := c.dec.DecodeInt64()
		if err != nil {
			return err
		}
		c.writeString(strconv.FormatInt(v, 10))

	case code == msgpcode.Float:
		v, err := c.dec.DecodeFloat32()
		if err != nil {
			return err
		}
		c.writeFloat(float64(v), 32)

	case code == msgpcode.Double:
		v, err := c.dec.DecodeFloat64()
		if err != nil {
			return err
		}
		c.writeFloat(v, 64)

	case msgpcode.IsString(code):
		v, err := c.dec.DecodeBytes()
		if err != nil {
			return err
		}
		if utf8.Valid(v) {
			c.writeString(string(v))
		} else {
			c.lossy = true
			c.writeBinary(v)
		}

	case msgpcode.IsBin(code):
		v, err := c.dec.DecodeBytes()
		if err != nil {
			return err
		}
		c.lossy = true
		c.writeBinary(v)

	case msgpcode.IsExt(code):
		return c.convertExt()

	case msgpcode.IsFixedArray(code) || code == msgpcode.Array16 || code == msgpcode.Array32:
		n, err := c.dec.DecodeArrayLen()
		if err != nil {
			return err
		}
		c.buf.WriteByte('[')
		for i := range n {
			if i > 0 {
				c.buf.WriteByte(',')
			}
			err = c.convert(depth + 1)
			if err != nil {
				return err
			}
		}
		c.buf.WriteByte(']')

	case msgpcode.IsFixedMap(code) || code == msgpcode.Map16 || code == msgpcode.Map32:
		return c.convertMap(depth)

	default:
		return fmt.Errorf("unsupported MessagePack code 0x%x", code)
	}

	return nil
}

func (c *converter) convertMap(depth int) error {
	n, err := c.dec.DecodeMapLen()
	if err != nil {
		return err
	}

	// The keys already written, since MessagePack allows repeated keys and different keys can share a rendering
	// The length comes from the header, so it only caps the size hint instead of setting it
	seen := make(map[string]struct{}, min(n, 64))

	c.buf.WriteByte('{')
	for i := range n {
		if i > 0 {
			c.buf.WriteByte(',')
		}

		// Keys that are valid UTF-8 strings are used as-is, and any other key is rendered as its JSON text
		code, err := c.dec.PeekCode()
		if err != nil {
			return err
		}
		var key string
		if msgpcode.IsString(code) {
			k, err := c.dec.DecodeBytes()
			if err != nil {
				return err
			}
			if utf8.Valid(k) {
				key = string(k)
			} else {
				c.lossy = true
				key = `{"$binary":"` + base64.StdEncoding.EncodeToString(k) + `"}`
			}
		} else {
			// Convert the key into a separate buffer, then use its text as the key
			// A key rendered as a JSON string, such as a number or a timestamp, is unquoted, so the integer key 1 becomes "1" rather than "\"1\""
			keyConv := &converter{r: c.r, dec: c.dec}
			err = keyConv.convert(depth + 1)
			if err != nil {
				return err
			}
			c.lossy = true
			key, err = keyText(keyConv.buf.Bytes())
			if err != nil {
				return err
			}
		}

		// A repeated name makes a JSON object that parsers read differently, usually keeping only the last value, so the map can't be rendered
		_, dup := seen[key]
		if dup {
			return fmt.Errorf("the map has more than one key rendered as the JSON name %q", key)
		}
		seen[key] = struct{}{}
		c.writeString(key)

		c.buf.WriteByte(':')
		err = c.convert(depth + 1)
		if err != nil {
			return err
		}
	}
	c.buf.WriteByte('}')

	return nil
}

func (c *converter) convertExt() error {
	extType, extLen, err := c.dec.DecodeExtHeader()
	if err != nil {
		return err
	}

	// The length comes from the header, and an extension can't be longer than the whole input, so a longer one is rejected before allocating or a few bytes could claim gigabytes
	if extLen < 0 || int64(extLen) > c.r.Size() {
		return fmt.Errorf("MessagePack extension of %d bytes exceeds the %d bytes of input", extLen, c.r.Size())
	}
	data := make([]byte, extLen)
	err = c.dec.ReadFull(data)
	if err != nil {
		return err
	}

	// Timestamps are rendered as RFC 3339 strings, and every other extension as its type and base64 data
	c.lossy = true
	if extType == timestampExtType {
		t, err := decodeTimestamp(data)
		if err != nil {
			return err
		}
		c.writeString(t.UTC().Format(time.RFC3339Nano))
		return nil
	}

	c.buf.WriteString(`{"$ext":`)
	c.buf.WriteString(strconv.Itoa(int(extType)))
	c.buf.WriteString(`,"data":"`)
	c.buf.WriteString(base64.StdEncoding.EncodeToString(data))
	c.buf.WriteString(`"}`)

	return nil
}

// decodeTimestamp decodes the payload of a timestamp extension in any of its three formats
func decodeTimestamp(data []byte) (time.Time, error) {
	switch len(data) {
	case 4:
		return time.Unix(int64(binary.BigEndian.Uint32(data)), 0), nil
	case 8:
		v := binary.BigEndian.Uint64(data)
		nsec := int64(v >> 34)
		sec := int64(v & 0x00000003ffffffff)
		return time.Unix(sec, nsec), nil
	case 12:
		nsec := int64(binary.BigEndian.Uint32(data[:4]))
		sec := int64(binary.BigEndian.Uint64(data[4:])) // #nosec G115 -- the 64-bit seconds field is signed by definition
		return time.Unix(sec, nsec), nil
	default:
		return time.Time{}, fmt.Errorf("invalid MessagePack timestamp length %d", len(data))
	}
}

func (c *converter) writeFloat(v float64, bitSize int) {
	switch {
	case math.IsNaN(v):
		c.writeString("NaN")
	case math.IsInf(v, 1):
		c.writeString("Infinity")
	case math.IsInf(v, -1):
		c.writeString("-Infinity")
	default:
		// The shortest representation for the float's own precision, so a float32 0.1 is "0.1" rather than the digits of its float64 widening
		c.writeString(strconv.FormatFloat(v, 'g', -1, bitSize))
	}
}

// keyText returns the text of a map key from its JSON rendering, unquoting it when it is a JSON string
func keyText(rendered []byte) (string, error) {
	if len(rendered) == 0 || rendered[0] != '"' {
		return string(rendered), nil
	}

	var key string
	err := json.Unmarshal(rendered, &key)
	if err != nil {
		return "", fmt.Errorf("failed to read a rendered map key: %w", err)
	}
	return key, nil
}

func (c *converter) writeBinary(v []byte) {
	c.buf.WriteString(`{"$binary":"`)
	c.buf.WriteString(base64.StdEncoding.EncodeToString(v))
	c.buf.WriteString(`"}`)
}

func (c *converter) writeString(s string) {
	// Marshaling a valid UTF-8 string never fails
	enc, _ := json.Marshal(s)
	c.buf.Write(enc)
}
