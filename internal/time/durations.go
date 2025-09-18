// This code was adapted from https://github.com/dapr/kit/tree/v0.15.4/
// Copyright (C) 2023 The Dapr Authors
// License: Apache2

//nolint:recvcheck
package time

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var errInvalidISO8601Duration = errors.New("unsupported ISO8601 duration format")

const zeroDuration = "PT0S"

// Duration represents a time duration.
type Duration struct {
	Years  int
	Months int
	Days   int
	Time   time.Duration
}

// FromString updates the Duration with the value from the string.
// It supports:
// - ISO8601 duration format
// - Go duration string format
func (d *Duration) FromString(from string) error {
	if from == "" {
		d.Reset()
		return nil
	}

	parsed, err := ParseDurationString(from)
	if err != nil {
		return err
	}

	*d = parsed
	return nil
}

func (d *Duration) Reset() {
	d.Years = 0
	d.Months = 0
	d.Days = 0
	d.Time = 0
}

func (d Duration) IsZero() bool {
	return d.Time < time.Millisecond && d.Days == 0 && d.Months == 0 && d.Years == 0
}

// String returns the ISO8601-formatted representation of the duration
func (d Duration) String() string {
	if d.IsZero() {
		return zeroDuration
	}

	b := strings.Builder{}
	b.WriteRune('P')

	if d.Years > 0 {
		b.WriteString(strconv.Itoa(d.Years))
		b.WriteRune('Y')
	}

	if d.Months > 0 {
		b.WriteString(strconv.Itoa(d.Months))
		b.WriteRune('M')
	}

	if d.Days > 0 {
		b.WriteString(strconv.Itoa(d.Days))
		b.WriteRune('D')
	}

	if d.Time >= time.Millisecond {
		b.WriteRune('T')

		hours := int(d.Time / time.Hour)
		minutes := int((d.Time % time.Hour) / time.Minute)
		seconds := int((d.Time % time.Minute) / time.Second)
		milliseconds := int((d.Time % time.Second) / time.Millisecond)

		if hours > 0 {
			b.WriteString(strconv.Itoa(hours))
			b.WriteRune('H')
		}

		if minutes > 0 {
			b.WriteString(strconv.Itoa(minutes))
			b.WriteRune('M')
		}

		if seconds > 0 || milliseconds > 0 {
			if milliseconds > 0 {
				b.WriteString(strconv.Itoa(seconds))
				b.WriteRune('.')
				b.WriteString(fmt.Sprintf("%03d", milliseconds))
			} else {
				b.WriteString(strconv.Itoa(seconds))
			}
			b.WriteRune('S')
		}
	}

	return b.String()
}

func ParseISO8601Duration(from string) (d Duration, err error) {
	// Length must be at least 2 characters per specs
	l := len(from)
	if l < 2 {
		return d, errInvalidISO8601Duration
	}

	// First character must be a "P"
	if from[0] != 'P' {
		return d, errInvalidISO8601Duration
	}

	var (
		i, start      = 1, 1
		isParsingTime bool
		isDecimal     bool
		tmp           int
	)
	for i < l {
		switch from[i] {
		case 'T':
			if start != i {
				return d, errInvalidISO8601Duration
			}
			isParsingTime = true
			start = i + 1

		case 'Y':
			if isParsingTime || isDecimal || start == i {
				return d, errInvalidISO8601Duration
			}
			d.Years, err = strconv.Atoi(from[start:i])
			if err != nil {
				return d, errInvalidISO8601Duration
			}
			start = i + 1

		case 'W':
			if isParsingTime || isDecimal || start == i {
				return d, errInvalidISO8601Duration
			}
			tmp, err = strconv.Atoi(from[start:i])
			if err != nil {
				return d, errInvalidISO8601Duration
			}
			d.Days += tmp * 7
			start = i + 1

		case 'D':
			if isParsingTime || isDecimal || start == i {
				return d, errInvalidISO8601Duration
			}
			tmp, err = strconv.Atoi(from[start:i])
			if err != nil {
				return d, errInvalidISO8601Duration
			}
			d.Days += tmp
			start = i + 1

		case 'H':
			if !isParsingTime || isDecimal || start == i {
				return d, errInvalidISO8601Duration
			}
			tmp, err = strconv.Atoi(from[start:i])
			if err != nil {
				return d, errInvalidISO8601Duration
			}
			d.Time += time.Duration(tmp) * time.Hour
			start = i + 1

		case 'S':
			if !isParsingTime || start == i {
				return d, errInvalidISO8601Duration
			}
			tmp, err = strconv.Atoi(from[start:i])
			if err != nil {
				return d, errInvalidISO8601Duration
			}

			if !isDecimal {
				d.Time += time.Duration(tmp) * time.Second
			} else {
				switch i - start {
				case 3:
					d.Time += time.Duration(tmp) * time.Millisecond
				case 2:
					d.Time += time.Duration(tmp*10) * time.Millisecond
				case 1:
					d.Time += time.Duration(tmp*100) * time.Millisecond
				default:
					return d, errInvalidISO8601Duration
				}
			}
			start = i + 1

		case 'M': // "M" can be used for both months and minutes
			if start == i || isDecimal {
				return d, errInvalidISO8601Duration
			}
			tmp, err = strconv.Atoi(from[start:i])
			if err != nil {
				return d, errInvalidISO8601Duration
			}
			if isParsingTime {
				d.Time += time.Duration(tmp) * time.Minute
			} else {
				d.Months = tmp
			}
			start = i + 1
		case '.':
			// We allow decimals only for seconds
			if !isParsingTime || isDecimal {
				return d, errInvalidISO8601Duration
			}

			tmp, err = strconv.Atoi(from[start:i])
			if err != nil {
				return d, errInvalidISO8601Duration
			}
			d.Time += time.Duration(tmp) * time.Second

			isDecimal = true
			start = i + 1
		}

		i++
	}

	return d, nil
}

// ParseDurationString creates Duration from either:
// - ISO8601 duration format
// - Go duration string format
func ParseDurationString(from string) (Duration, error) {
	d, err := ParseISO8601Duration(from)
	if err == nil {
		return d, nil
	}

	// Reset to be safe
	d.Reset()

	d.Time, err = time.ParseDuration(from)
	if err == nil {
		return d, nil
	}

	return d, errors.New("unsupported duration format")
}
