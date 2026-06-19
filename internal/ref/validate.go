package ref

import (
	"errors"
	"strings"
)

// ErrInvalidRefComponent is returned when an actor type, actor ID, or alarm name contains a reserved character.
var ErrInvalidRefComponent = errors.New("actor type, actor ID, and alarm name must not contain '/'")

// ValidateComponents returns ErrInvalidRefComponent if any of the given strings contain "/" because that character is used as the key-join delimiter and its presence would create key collisions
func ValidateComponents(ss ...string) error {
	for _, s := range ss {
		if strings.ContainsRune(s, '/') {
			return ErrInvalidRefComponent
		}
	}
	return nil
}
