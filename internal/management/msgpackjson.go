package management

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

// maxSafeInteger is the largest integer a JSON number holds exactly in IEEE 754 double precision (2^53)
const maxSafeInteger = 1 << 53

// maxMsgpackDepth bounds the nesting of converted values, so a hostile value cannot exhaust the stack
const maxMsgpackDepth = 512

// timestampExtType is the MessagePack extension type of timestamps
const timestampExtType = -1

// msgpackToJSON converts a MessagePack value to JSON, using the documented convention for values without a JSON equivalent
// lossy is true when any value used a rendering that cannot be converted back to the same MessagePack bytes
func msgpackToJSON(data []byte) (out json.RawMessage, lossy bool, err error) {
	r := bytes.NewReader(data)
	c := &msgpackConverter{
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

type msgpackConverter struct {
	// r is the input the decoder reads from, which tells how many bytes remain
	r     *bytes.Reader
	dec   *msgpack.Decoder
	buf   bytes.Buffer
	lossy bool
}

func (c *msgpackConverter) convert(depth int) error {
	if depth > maxMsgpackDepth {
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

	case code == msgpcode.Uint64:
		v, err := c.dec.DecodeUint64()
		if err != nil {
			return err
		}
		if v > maxSafeInteger {
			c.lossy = true
			c.writeString(strconv.FormatUint(v, 10))
		} else {
			c.buf.WriteString(strconv.FormatUint(v, 10))
		}

	case msgpcode.IsFixedNum(code),
		code == msgpcode.Uint8, code == msgpcode.Uint16, code == msgpcode.Uint32,
		code == msgpcode.Int8, code == msgpcode.Int16, code == msgpcode.Int32, code == msgpcode.Int64:
		v, err := c.dec.DecodeInt64()
		if err != nil {
			return err
		}
		if v > maxSafeInteger || v < -maxSafeInteger {
			c.lossy = true
			c.writeString(strconv.FormatInt(v, 10))
		} else {
			c.buf.WriteString(strconv.FormatInt(v, 10))
		}

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

func (c *msgpackConverter) convertMap(depth int) error {
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
			// Convert the key into a separate buffer, then use its JSON text as the key
			keyConv := &msgpackConverter{r: c.r, dec: c.dec}
			err = keyConv.convert(depth + 1)
			if err != nil {
				return err
			}
			c.lossy = true
			key = keyConv.buf.String()
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

func (c *msgpackConverter) convertExt() error {
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
		t, err := decodeMsgpackTimestamp(data)
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

// decodeMsgpackTimestamp decodes the payload of a timestamp extension in any of its three formats
func decodeMsgpackTimestamp(data []byte) (time.Time, error) {
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

func (c *msgpackConverter) writeFloat(v float64, bitSize int) {
	switch {
	case math.IsNaN(v):
		c.lossy = true
		c.writeString("NaN")
	case math.IsInf(v, 1):
		c.lossy = true
		c.writeString("Infinity")
	case math.IsInf(v, -1):
		c.lossy = true
		c.writeString("-Infinity")
	default:
		c.buf.WriteString(strconv.FormatFloat(v, 'g', -1, bitSize))
	}
}

func (c *msgpackConverter) writeBinary(v []byte) {
	c.buf.WriteString(`{"$binary":"`)
	c.buf.WriteString(base64.StdEncoding.EncodeToString(v))
	c.buf.WriteString(`"}`)
}

func (c *msgpackConverter) writeString(s string) {
	// Marshaling a valid UTF-8 string never fails
	enc, _ := json.Marshal(s)
	c.buf.Write(enc)
}
