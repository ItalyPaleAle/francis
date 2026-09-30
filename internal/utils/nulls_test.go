package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNullString(t *testing.T) {
	assert.Nil(t, NullString(""))
	assert.Equal(t, "value", NullString("value"))
}

func TestNullBytes(t *testing.T) {
	assert.Nil(t, NullBytes(nil))
	assert.Nil(t, NullBytes([]byte{}))
	assert.Equal(t, []byte("value"), NullBytes([]byte("value")))
}
