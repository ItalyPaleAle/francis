package ref

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateComponents(t *testing.T) {
	require.NoError(t, ValidateComponents("mytype", "myid"))
	require.NoError(t, ValidateComponents("mytype", "myid", "myalarm"))
	require.NoError(t, ValidateComponents())

	// A slash in any component is rejected to prevent key collisions
	require.ErrorIs(t, ValidateComponents("my/type", "id"), ErrInvalidRefComponent)
	require.ErrorIs(t, ValidateComponents("type", "my/id"), ErrInvalidRefComponent)
	require.ErrorIs(t, ValidateComponents("type", "id", "alarm/name"), ErrInvalidRefComponent)

	// An empty component names nothing, so it is rejected in every position
	require.ErrorIs(t, ValidateComponents(""), ErrInvalidRefComponent)
	require.ErrorIs(t, ValidateComponents("", "id"), ErrInvalidRefComponent)
	require.ErrorIs(t, ValidateComponents("type", ""), ErrInvalidRefComponent)
	require.ErrorIs(t, ValidateComponents("type", "id", ""), ErrInvalidRefComponent)
}
