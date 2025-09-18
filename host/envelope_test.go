package host

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/actors/actor"
)

// Compile-time interface assertion
var _ actor.Envelope = (*objectEnvelope)(nil)

func TestObjectEnvelope(t *testing.T) {
	t.Run("nil envelope returns nil", func(t *testing.T) {
		var envelope *objectEnvelope
		var target string

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, "", target) // target should remain unchanged
	})

	t.Run("decode nil object returns nil", func(t *testing.T) {
		envelope := newObjectEnvelope(nil)
		var target string

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, "", target) // target should remain unchanged
	})

	t.Run("decode into nil target returns error", func(t *testing.T) {
		envelope := newObjectEnvelope("test")

		err := envelope.Decode(nil)

		require.Error(t, err)
		require.ErrorContains(t, err, "target object is nil")
	})

	t.Run("decode into non-pointer returns error", func(t *testing.T) {
		envelope := newObjectEnvelope("test")
		var target string

		err := envelope.Decode(target) // passing value instead of pointer

		require.Error(t, err)
		require.ErrorContains(t, err, "parameter out must be a non-nil pointer")
	})

	t.Run("decode into nil pointer returns error", func(t *testing.T) {
		envelope := newObjectEnvelope("test")
		var target *string // nil pointer

		err := envelope.Decode(target)

		require.Error(t, err)
		require.ErrorContains(t, err, "parameter out must be a non-nil pointer")
	})

	t.Run("decode zero value object returns nil", func(t *testing.T) {
		var zeroValue int
		envelope := newObjectEnvelope(zeroValue)
		var target int

		err := envelope.Decode(&target)

		require.NoError(t, err)
	})

	t.Run("fast path - direct assignment for same type", func(t *testing.T) {
		original := "test string"
		envelope := newObjectEnvelope(original)
		var target string

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, original, target)
	})

	t.Run("fast path - direct assignment for assignable types", func(t *testing.T) {
		type customString string
		original := customString("test")
		envelope := newObjectEnvelope(original)
		var target customString

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, original, target)
	})

	t.Run("fast path - int to int assignment", func(t *testing.T) {
		original := 42
		envelope := newObjectEnvelope(original)
		var target int

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, original, target)
	})

	t.Run("msgpack serialization path for struct", func(t *testing.T) {
		type testStruct struct {
			Name string `msgpack:"name"`
			Age  int    `msgpack:"age"`
		}

		original := testStruct{Name: "John", Age: 30}
		envelope := newObjectEnvelope(original)
		var target testStruct

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, original, target)
	})

	t.Run("msgpack serialization path for map", func(t *testing.T) {
		original := map[string]interface{}{
			"key1": "value1",
			"key2": 42,
		}
		envelope := newObjectEnvelope(original)
		var target map[string]interface{}

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, original, target)
	})

	t.Run("msgpack serialization path for slice", func(t *testing.T) {
		original := []string{"a", "b", "c"}
		envelope := newObjectEnvelope(original)
		var target []string

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, original, target)
	})

	t.Run("msgpack serialization path - type conversion", func(t *testing.T) {
		// Test case where types are not directly assignable but msgpack can handle conversion
		original := map[string]string{
			"name": "John",
			"city": "New York",
		}

		type Person struct {
			Name string `msgpack:"name"`
			City string `msgpack:"city"`
		}

		envelope := newObjectEnvelope(original)
		var target Person

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, "John", target.Name)
		assert.Equal(t, "New York", target.City)
	})

	t.Run("msgpack deserialization failure", func(t *testing.T) {
		// Create a scenario where serialization works but deserialization fails
		original := "not a number"
		envelope := newObjectEnvelope(original)
		var target int // trying to decode string into int

		err := envelope.Decode(&target)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to deserialize state using msgpack")
	})

	t.Run("complex nested struct", func(t *testing.T) {
		type Address struct {
			Street string `msgpack:"street"`
			City   string `msgpack:"city"`
		}

		type Person struct {
			Name    string  `msgpack:"name"`
			Age     int     `msgpack:"age"`
			Address Address `msgpack:"address"`
		}

		original := Person{
			Name: "Alice",
			Age:  25,
			Address: Address{
				Street: "123 Main St",
				City:   "Boston",
			},
		}

		envelope := newObjectEnvelope(original)
		var target Person

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, original, target)
	})

	t.Run("pointer types", func(t *testing.T) {
		original := "test string"
		envelope := newObjectEnvelope(&original)
		var target *string

		err := envelope.Decode(&target)

		require.NoError(t, err)
		require.NotNil(t, target)
		assert.Equal(t, original, *target)
	})

	t.Run("any types", func(t *testing.T) {
		original := any("test string")
		envelope := newObjectEnvelope(original)
		var target any

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, original, target)
	})

	t.Run("zero value non-zero type", func(t *testing.T) {
		// Test with a type that has a non-zero value as zero (like empty slice)
		var original []string // nil slice
		envelope := newObjectEnvelope(original)
		var target []string

		err := envelope.Decode(&target)

		require.NoError(t, err)
		// Both should be nil/empty
		assert.Nil(t, target)
	})

	t.Run("boolean values", func(t *testing.T) {
		tests := []bool{true, false}

		for _, original := range tests {
			t.Run(fmt.Sprintf("boolean_%t", original), func(t *testing.T) {
				envelope := newObjectEnvelope(original)
				var target bool

				err := envelope.Decode(&target)

				require.NoError(t, err)
				assert.Equal(t, original, target)
			})
		}
	})

	t.Run("large data structure", func(t *testing.T) {
		// Test with a large data structure to ensure msgpack handles it properly
		original := make(map[string][]int)
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key%d", i)
			original[key] = make([]int, 100)
			for j := 0; j < 100; j++ {
				original[key][j] = i*100 + j
			}
		}

		envelope := newObjectEnvelope(original)
		var target map[string][]int

		err := envelope.Decode(&target)

		require.NoError(t, err)
		assert.Equal(t, len(original), len(target))
		// Spot check a few values
		assert.Equal(t, original["key0"], target["key0"])
		assert.Equal(t, original["key999"], target["key999"])
	})

	t.Run("decode same envelope multiple times", func(t *testing.T) {
		original := map[string]int{"a": 1, "b": 2}
		envelope := newObjectEnvelope(original)

		// Decode multiple times to ensure envelope is reusable
		var target1, target2 map[string]int

		err1 := envelope.Decode(&target1)
		err2 := envelope.Decode(&target2)

		require.NoError(t, err1)
		require.NoError(t, err2)
		assert.Equal(t, original, target1)
		assert.Equal(t, original, target2)
		assert.Equal(t, target1, target2)
	})
}

func BenchmarkObjectEnvelope(b *testing.B) {
	b.Run("fast path string", func(b *testing.B) {
		envelope := newObjectEnvelope("test string")
		var target string

		b.ResetTimer()
		for b.Loop() {
			_ = envelope.Decode(&target)
		}
	})

	b.Run("fast path struct", func(b *testing.B) {
		type testStruct struct {
			Name string `msgpack:"name"`
			Age  int    `msgpack:"age"`
		}

		envelope := newObjectEnvelope(testStruct{Name: "John", Age: 30})
		var target testStruct

		b.ResetTimer()
		for b.Loop() {
			_ = envelope.Decode(&target)
		}
	})

	b.Run("fast path map", func(b *testing.B) {
		original := map[string]any{
			"key1": "value1",
			"key2": 42,
			"key3": []string{"a", "b", "c"},
		}
		envelope := newObjectEnvelope(original)
		var target map[string]any

		b.ResetTimer()
		for b.Loop() {
			target = make(map[string]any) // Reset target
			_ = envelope.Decode(&target)
		}
	})

	b.Run("msgpack path", func(b *testing.B) {
		type testStruct struct {
			Name string `msgpack:"name"`
			Age  int    `msgpack:"age"`
		}

		envelope := newObjectEnvelope(testStruct{Name: "John", Age: 30})
		var target map[string]any

		b.ResetTimer()
		for b.Loop() {
			_ = envelope.Decode(&target)
		}
	})
}
