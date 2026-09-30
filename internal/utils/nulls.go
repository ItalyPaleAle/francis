package utils

// NullString returns nil for an empty string, otherwise the string
// It lets callers pass an empty value as absent where a parameter of type any is expected
func NullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// NullBytes returns nil for an empty byte slice, otherwise the slice
// It lets callers pass an empty value as absent where a parameter of type any is expected
func NullBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}
