package management

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"net"
)

// DefaultBind is the listen address used when the configuration does not set one
const DefaultBind = "127.0.0.1:7401"

// MinTokenLength is the minimum length of an API token
// It keeps the last characters used to identify a token in audit logs from revealing a meaningful part of it
const MinTokenLength = 32

// tokenSuffixLength is the number of trailing characters that identify a token in audit logs
const tokenSuffixLength = 5

// Config configures the management API server
type Config struct {
	// Bind is the TCP address the server listens on
	Bind string
	// ReadOnlyTokens receive every scope except those ending in ":manage"
	ReadOnlyTokens []string
	// ManagementTokens receive every scope
	ManagementTokens []string
	// TLSConfig, when set, makes the server serve HTTPS directly
	TLSConfig *tls.Config
	// Dashboard, when set, holds the compiled dashboard, which is served at the root of the listener
	// It must contain an index.html file at its root
	Dashboard fs.FS
	// AllowedOrigins lists the browser origins that may call the API, such as a dashboard served by the "francis dashboard" command
	// Each is a scheme, host, and optional port, such as "http://localhost:7402", or "*" for any origin
	AllowedOrigins []string
}

// Validate checks the configuration and applies defaults
func (c *Config) Validate() error {
	if c.Bind == "" {
		c.Bind = DefaultBind
	}

	_, _, err := net.SplitHostPort(c.Bind)
	if err != nil {
		return fmt.Errorf("invalid management bind address '%s': %w", c.Bind, err)
	}

	if len(c.ReadOnlyTokens) == 0 && len(c.ManagementTokens) == 0 {
		return errors.New("the management API requires at least one read-only or management token")
	}

	// Every token must be long enough and appear only once across both lists
	seen := make(map[string]struct{}, len(c.ReadOnlyTokens)+len(c.ManagementTokens))
	check := func(list string, tokens []string) error {
		for i, t := range tokens {
			if len(t) < MinTokenLength {
				return fmt.Errorf("management %s token at index %d is shorter than %d characters", list, i, MinTokenLength)
			}

			_, dup := seen[t]
			if dup {
				return fmt.Errorf("management %s token at index %d is a duplicate: every token must be unique across both lists", list, i)
			}

			seen[t] = struct{}{}
		}
		return nil
	}

	err = check("read-only", c.ReadOnlyTokens)
	if err != nil {
		return err
	}

	err = check("management", c.ManagementTokens)
	if err != nil {
		return err
	}

	// Origins are compared as browsers send them, so the configured ones are normalized to the same form
	if len(c.AllowedOrigins) > 0 {
		origins := make([]string, len(c.AllowedOrigins))
		for i, o := range c.AllowedOrigins {
			origins[i], err = normalizeOrigin(o)
			if err != nil {
				return fmt.Errorf("management allowed origin at index %d is invalid: %w", i, err)
			}
		}

		c.AllowedOrigins = origins
	}

	return nil
}
