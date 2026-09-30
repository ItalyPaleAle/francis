package msgpackjson

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// mp builds MessagePack bytes by concatenating raw byte slices, so each case shows the exact wire format it tests
func mp(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

func b(v ...byte) []byte {
	return v
}

func be32(v uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, v)
}

func be64(v uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, v)
}

func bei64(v int64) []byte {
	return be64(uint64(v)) // #nosec G115 -- the bit pattern of a signed value is what is being encoded
}

// fixstr encodes a short string with the fixstr format
func fixstr(s string) []byte {
	return append([]byte{0xa0 | byte(len(s))}, s...) // #nosec G115 -- fixstr fixtures are shorter than 32 bytes
}

// encode encodes a value with the msgpack library
func encode(t *testing.T, fn func(enc *msgpack.Encoder) error) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	require.NoError(t, fn(enc))
	return buf.Bytes()
}

func TestToJSON(t *testing.T) {
	tests := []struct {
		name  string
		in    []byte
		want  string
		lossy bool
	}{
		// Scalars
		{name: "nil", in: b(0xc0), want: `null`},
		{name: "true", in: b(0xc3), want: `true`},
		{name: "false", in: b(0xc2), want: `false`},

		// Integers of every encoding and size are decimal strings, which never makes the result lossy
		{name: "positive fixint", in: b(0x05), want: `"5"`},
		{name: "negative fixint", in: b(0xff), want: `"-1"`},
		{name: "uint8", in: b(0xcc, 0xff), want: `"255"`},
		{name: "uint16", in: b(0xcd, 0x01, 0x00), want: `"256"`},
		{name: "uint32", in: mp(b(0xce), be32(math.MaxUint32)), want: `"4294967295"`},
		{name: "int8", in: b(0xd0, 0x80), want: `"-128"`},
		{name: "int16", in: b(0xd1, 0x80, 0x00), want: `"-32768"`},
		{name: "int32", in: mp(b(0xd2), be32(0x80000000)), want: `"-2147483648"`},
		{name: "uint64 small", in: mp(b(0xcf), be64(42)), want: `"42"`},
		{name: "uint64 over 2^53", in: mp(b(0xcf), be64(1<<53+1)), want: `"9007199254740993"`},
		{name: "uint64 max", in: mp(b(0xcf), be64(math.MaxUint64)), want: `"18446744073709551615"`},
		{name: "int64 over 2^53", in: mp(b(0xd3), be64(1<<53+1)), want: `"9007199254740993"`},
		{name: "int64 under -2^53", in: mp(b(0xd3), bei64(-(1<<53)-1)), want: `"-9007199254740993"`},
		{name: "int64 min", in: mp(b(0xd3), be64(1<<63)), want: `"-9223372036854775808"`},
		{name: "int64 max", in: mp(b(0xd3), be64(math.MaxInt64)), want: `"9223372036854775807"`},

		// Floats are strings too, including NaN and the infinities
		{name: "float32", in: mp(b(0xca), be32(math.Float32bits(1.5))), want: `"1.5"`},
		{name: "float32 keeps its shortest representation", in: mp(b(0xca), be32(math.Float32bits(0.1))), want: `"0.1"`},
		{name: "float64", in: mp(b(0xcb), be64(math.Float64bits(0.1))), want: `"0.1"`},
		{name: "float64 negative exponent", in: mp(b(0xcb), be64(math.Float64bits(-1.25e-10))), want: `"-1.25e-10"`},
		{name: "float64 large exponent", in: mp(b(0xcb), be64(math.Float64bits(1e21))), want: `"1e+21"`},
		{name: "float64 zero", in: mp(b(0xcb), be64(math.Float64bits(0))), want: `"0"`},
		{name: "float64 NaN", in: mp(b(0xcb), be64(math.Float64bits(math.NaN()))), want: `"NaN"`},
		{name: "float64 +Inf", in: mp(b(0xcb), be64(math.Float64bits(math.Inf(1)))), want: `"Infinity"`},
		{name: "float64 -Inf", in: mp(b(0xcb), be64(math.Float64bits(math.Inf(-1)))), want: `"-Infinity"`},
		{name: "float32 NaN", in: mp(b(0xca), be32(math.Float32bits(float32(math.NaN())))), want: `"NaN"`},
		{name: "float32 +Inf", in: mp(b(0xca), be32(math.Float32bits(float32(math.Inf(1))))), want: `"Infinity"`},
		{name: "float32 -Inf", in: mp(b(0xca), be32(math.Float32bits(float32(math.Inf(-1))))), want: `"-Infinity"`},

		// Strings
		{name: "empty string", in: b(0xa0), want: `""`},
		{name: "fixstr", in: fixstr("hello"), want: `"hello"`},
		{name: "str8 with unicode", in: mp(b(0xd9, byte(len("héllo, ünïcødé"))), []byte("héllo, ünïcødé")), want: `"héllo, ünïcødé"`},
		{name: "string with characters to escape", in: fixstr("a\"b\\c\n"), want: `"a\"b\\c\n"`},
		{name: "invalid UTF-8 string", in: b(0xa3, 0xff, 0xfe, 0x41), want: `{"$binary":"//5B"}`, lossy: true},

		// Binary
		{name: "bin8", in: b(0xc4, 0x03, 0x01, 0x02, 0x03), want: `{"$binary":"AQID"}`, lossy: true},
		{name: "empty bin", in: b(0xc4, 0x00), want: `{"$binary":""}`, lossy: true},
		{name: "bin16", in: mp(b(0xc5, 0x00, 0x02), b(0xde, 0xad)), want: `{"$binary":"3q0="}`, lossy: true},

		// Timestamps in each of the three formats
		{name: "timestamp32", in: mp(b(0xd6, 0xff), be32(1700000000)), want: `"2023-11-14T22:13:20Z"`, lossy: true},
		{name: "timestamp32 epoch", in: mp(b(0xd6, 0xff), be32(0)), want: `"1970-01-01T00:00:00Z"`, lossy: true},
		{name: "timestamp64", in: mp(b(0xd7, 0xff), be64(uint64(500000000)<<34|1700000000)), want: `"2023-11-14T22:13:20.5Z"`, lossy: true},
		{name: "timestamp64 nanoseconds", in: mp(b(0xd7, 0xff), be64(uint64(123456789)<<34|1700000000)), want: `"2023-11-14T22:13:20.123456789Z"`, lossy: true},
		{name: "timestamp96", in: mp(b(0xc7, 0x0c, 0xff), be32(1), be64(1700000000)), want: `"2023-11-14T22:13:20.000000001Z"`, lossy: true},
		{name: "timestamp96 before the epoch", in: mp(b(0xc7, 0x0c, 0xff), be32(0), be64(uint64(0xffffffffffffffff))), want: `"1969-12-31T23:59:59Z"`, lossy: true},

		// Other extensions
		{name: "fixext1", in: b(0xd4, 0x05, 0xaa), want: `{"$ext":5,"data":"qg=="}`, lossy: true},
		{name: "ext8 with negative type", in: b(0xc7, 0x02, 0xfe, 0x01, 0x02), want: `{"$ext":-2,"data":"AQI="}`, lossy: true},
		{name: "ext8 empty", in: b(0xc7, 0x00, 0x10), want: `{"$ext":16,"data":""}`, lossy: true},

		// Arrays
		{name: "empty array", in: b(0x90), want: `[]`},
		{name: "array of mixed values", in: mp(b(0x93, 0x01), fixstr("a"), b(0xc0)), want: `["1","a",null]`},
		{name: "nested arrays", in: mp(b(0x92, 0x92, 0x01, 0x91, 0x02, 0x90)), want: `[["1",["2"]],[]]`},
		{name: "array16", in: b(0xdc, 0x00, 0x02, 0xc3, 0xc2), want: `[true,false]`},
		{name: "array with a lossy element", in: mp(b(0x92, 0x01), b(0xc4, 0x01, 0x00)), want: `["1",{"$binary":"AA=="}]`, lossy: true},

		// Maps
		{name: "empty map", in: b(0x80), want: `{}`},
		{name: "map preserves key order", in: mp(b(0x83), fixstr("z"), b(0x01), fixstr("a"), b(0x02), fixstr("m"), b(0x03)), want: `{"z":"1","a":"2","m":"3"}`},
		{name: "map16", in: mp(b(0xde, 0x00, 0x01), fixstr("k"), fixstr("v")), want: `{"k":"v"}`},
		{name: "nested map and array", in: mp(b(0x82), fixstr("b"), b(0x81), fixstr("c"), b(0x92, 0x01, 0x02), fixstr("a"), b(0xc0)), want: `{"b":{"c":["1","2"]},"a":null}`},
		{name: "map with escaped key", in: mp(b(0x81), fixstr("a\"b"), b(0x01)), want: `{"a\"b":"1"}`},

		// Keys that aren't strings become their text, and rendered strings are unquoted so they don't nest quotes
		{name: "integer key", in: mp(b(0x81, 0x01), fixstr("a")), want: `{"1":"a"}`, lossy: true},
		{name: "float key", in: mp(b(0x81, 0xcb), be64(math.Float64bits(1.5)), fixstr("a")), want: `{"1.5":"a"}`, lossy: true},
		{name: "timestamp key", in: mp(b(0x81, 0xd6, 0xff), be32(0), fixstr("a")), want: `{"1970-01-01T00:00:00Z":"a"}`, lossy: true},
		{name: "bool key", in: mp(b(0x81, 0xc3, 0x01)), want: `{"true":"1"}`, lossy: true},
		{name: "nil key", in: mp(b(0x81, 0xc0, 0x01)), want: `{"null":"1"}`, lossy: true},
		{name: "array key", in: mp(b(0x81, 0x92, 0x01, 0x02), b(0x03)), want: `{"[\"1\",\"2\"]":"3"}`, lossy: true},
		{name: "map key", in: mp(b(0x81, 0x81), fixstr("x"), b(0x01, 0x02)), want: `{"{\"x\":\"1\"}":"2"}`, lossy: true},
		{name: "invalid UTF-8 key", in: mp(b(0x81, 0xa1, 0xff, 0x01)), want: `{"{\"$binary\":\"/w==\"}":"1"}`, lossy: true},
		{name: "map with lossy value", in: mp(b(0x81), fixstr("ts"), b(0xd6, 0xff), be32(0)), want: `{"ts":"1970-01-01T00:00:00Z"}`, lossy: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, lossy, err := ToJSON(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, string(out))
			assert.Equal(t, tc.lossy, lossy)
			assert.True(t, json.Valid(out), "output is not valid JSON: %s", out)
		})
	}
}

func TestToJSONLibraryEncoded(t *testing.T) {
	t.Run("struct encoded by the library", func(t *testing.T) {
		type inner struct {
			Tags []string `msgpack:"tags"`
		}
		type value struct {
			Name  string  `msgpack:"name"`
			Count int     `msgpack:"count"`
			Ratio float64 `msgpack:"ratio"`
			Inner inner   `msgpack:"inner"`
		}
		data, err := msgpack.Marshal(value{Name: "n", Count: 3, Ratio: 0.5, Inner: inner{Tags: []string{"x", "y"}}})
		require.NoError(t, err)

		out, lossy, err := ToJSON(data)
		require.NoError(t, err)
		assert.JSONEq(t, `{"name":"n","count":"3","ratio":"0.5","inner":{"tags":["x","y"]}}`, string(out))
		assert.False(t, lossy)
	})

	t.Run("uint64 max encoded by the library", func(t *testing.T) {
		data := encode(t, func(enc *msgpack.Encoder) error {
			return enc.EncodeUint(math.MaxUint64)
		})
		out, lossy, err := ToJSON(data)
		require.NoError(t, err)
		assert.Equal(t, `"18446744073709551615"`, string(out))
		assert.False(t, lossy)
	})
}

func TestToJSONErrors(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		_, _, err := ToJSON(nil)
		require.Error(t, err)
	})

	t.Run("trailing data", func(t *testing.T) {
		_, _, err := ToJSON(b(0xc0, 0xc0))
		require.Error(t, err)
		assert.ErrorContains(t, err, "trailing data")
	})

	// Each map has two keys that would become the same JSON name
	collisions := []struct {
		name string
		in   []byte
		key  string
	}{
		{name: "integer and string keys", in: mp(b(0x82, 0x01), fixstr("a"), fixstr("1"), fixstr("b")), key: `"1"`},
		{name: "repeated string key", in: mp(b(0x82), fixstr("a"), b(0x01), fixstr("a"), b(0x02)), key: `"a"`},
		{name: "boolean and string keys", in: mp(b(0x82, 0xc3, 0x01), fixstr("true"), b(0x02)), key: `"true"`},
		{name: "binary key and its rendering as a string", in: mp(b(0x82, 0xc4, 0x01, 0x41, 0x01), fixstr(`{"$binary":"QQ=="}`), b(0x02)), key: `"{\"$binary\":\"QQ==\"}"`},
		{name: "colliding keys in a nested map", in: mp(b(0x81), fixstr("m"), b(0x82, 0x01, 0x01), fixstr("1"), b(0x02)), key: `"1"`},
	}
	for _, tc := range collisions {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ToJSON(tc.in)
			require.Error(t, err)
			assert.ErrorContains(t, err, "more than one key rendered as the JSON name "+tc.key)
		})
	}

	t.Run("trailing data after a map", func(t *testing.T) {
		_, _, err := ToJSON(mp(b(0x81), fixstr("a"), b(0x01, 0x02)))
		require.Error(t, err)
		assert.ErrorContains(t, err, "trailing data")
	})

	t.Run("truncated string", func(t *testing.T) {
		_, _, err := ToJSON(b(0xa5, 'a', 'b'))
		require.Error(t, err)
	})

	t.Run("truncated array", func(t *testing.T) {
		_, _, err := ToJSON(b(0x92, 0x01))
		require.Error(t, err)
	})

	t.Run("truncated map value", func(t *testing.T) {
		_, _, err := ToJSON(mp(b(0x81), fixstr("a")))
		require.Error(t, err)
	})

	t.Run("truncated extension", func(t *testing.T) {
		_, _, err := ToJSON(b(0xc7, 0x05, 0x01, 0x00))
		require.Error(t, err)
	})

	t.Run("extension longer than the input", func(t *testing.T) {
		// An ext32 header claiming almost 4 GiB is rejected before anything is allocated for it
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		_, _, err := ToJSON(mp(b(0xc9), be32(0xfffffff0), b(0x01, 0x00)))
		runtime.ReadMemStats(&after)
		require.ErrorContains(t, err, "exceeds the 7 bytes of input")
		assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "the declared length must not be allocated")
	})

	t.Run("never-used code", func(t *testing.T) {
		_, _, err := ToJSON(b(0xc1))
		require.Error(t, err)
		assert.ErrorContains(t, err, "unsupported MessagePack code 0xc1")
	})

	t.Run("invalid timestamp length", func(t *testing.T) {
		_, _, err := ToJSON(b(0xc7, 0x03, 0xff, 0x00, 0x00, 0x00))
		require.Error(t, err)
		assert.ErrorContains(t, err, "invalid MessagePack timestamp length 3")
	})

	t.Run("max depth", func(t *testing.T) {
		// Each fixarray of one element adds a level, and the innermost nil sits at the depth equal to the number of arrays
		nested := func(levels int) []byte {
			return append(bytes.Repeat(b(0x91), levels), 0xc0)
		}

		out, _, err := ToJSON(nested(maxDepth))
		require.NoError(t, err)
		assert.Equal(t, strings.Repeat("[", maxDepth)+"null"+strings.Repeat("]", maxDepth), string(out))

		_, _, err = ToJSON(nested(maxDepth + 1))
		require.Error(t, err)
		assert.ErrorContains(t, err, "nested too deeply")
	})

	t.Run("max depth through map values", func(t *testing.T) {
		data := append(bytes.Repeat(mp(b(0x81), fixstr("k")), maxDepth+1), 0xc0)
		_, _, err := ToJSON(data)
		require.Error(t, err)
		assert.ErrorContains(t, err, "nested too deeply")
	})

	t.Run("max depth through map keys", func(t *testing.T) {
		data := append(bytes.Repeat(b(0x81), maxDepth+1), 0xc0)
		_, _, err := ToJSON(data)
		require.Error(t, err)
		assert.ErrorContains(t, err, "nested too deeply")
	})
}

func TestDecodeTimestamp(t *testing.T) {
	ts, err := decodeTimestamp(be32(1))
	require.NoError(t, err)
	assert.Equal(t, int64(1), ts.Unix())

	ts, err = decodeTimestamp(be64(uint64(7)<<34 | 2))
	require.NoError(t, err)
	assert.Equal(t, int64(2), ts.Unix())
	assert.Equal(t, 7, ts.Nanosecond())

	ts, err = decodeTimestamp(mp(be32(9), be64(3)))
	require.NoError(t, err)
	assert.Equal(t, int64(3), ts.Unix())
	assert.Equal(t, 9, ts.Nanosecond())

	for _, n := range []int{0, 1, 5, 16} {
		_, err = decodeTimestamp(make([]byte, n))
		require.Error(t, err)
	}
}
