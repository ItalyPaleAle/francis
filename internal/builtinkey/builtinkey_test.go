package builtinkey

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKey(t *testing.T) {
	t.Run("New produces a valid key", func(t *testing.T) {
		require.NotPanics(t, func() {
			New().MustBeValid()
		})
	})

	t.Run("unnamed empty struct is not assignable to Key", func(t *testing.T) {
		emptyStructType := reflect.TypeFor[struct{}]()
		keyType := reflect.TypeFor[Key]()
		assert.False(t, emptyStructType.AssignableTo(keyType))
		assert.False(t, emptyStructType.ConvertibleTo(keyType))
	})

	t.Run("zero value panics", func(t *testing.T) {
		assert.PanicsWithValue(t, "builtinkey: unauthorized or zero-value Key", func() {
			var zero Key
			zero.MustBeValid()
		})
	})

	t.Run("reflect.Zero value panics", func(t *testing.T) {
		forged, ok := reflect.TypeAssert[Key](reflect.Zero(reflect.TypeFor[Key]()))
		require.True(t, ok)
		assert.PanicsWithValue(t, "builtinkey: unauthorized or zero-value Key", func() {
			forged.MustBeValid()
		})
	})
}
