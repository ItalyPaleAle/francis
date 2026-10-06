package management

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// normalizeOrigin returns an origin in the form browsers send in the Origin header, such as "https://example.com:8443"
// The scheme and host are lowercased and a default port is dropped, and "*" is kept as-is
func normalizeOrigin(origin string) (string, error) {
	origin = strings.TrimSpace(origin)
	if origin == "*" {
		return origin, nil
	}

	u, err := url.Parse(origin)
	if err != nil {
		return "", err
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", errors.New("the scheme must be http or https")
	}
	if u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("an origin is a scheme, a host, and an optional port, with nothing else")
	}

	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		// An IPv6 literal keeps its brackets even without a port
		host = "[" + host + "]"
	}

	return scheme + "://" + host, nil
}

// corsPolicy decides which browser origins may call the API
type corsPolicy struct {
	any     bool
	origins map[string]struct{}
}

func newCORSPolicy(origins []string) corsPolicy {
	p := corsPolicy{
		origins: make(map[string]struct{}, len(origins)),
	}
	for _, o := range origins {
		if o == "*" {
			p.any = true
		}
		p.origins[o] = struct{}{}
	}
	return p
}

// allows reports whether the origin may call the API
func (p corsPolicy) allows(origin string) bool {
	if p.any {
		return true
	}

	normalized, err := normalizeOrigin(origin)
	if err != nil || normalized == "*" {
		return false
	}
	_, ok := p.origins[normalized]
	return ok
}

// middlewareCORS lets the browser origins in the configuration call the API, such as a dashboard served by the "francis dashboard" command
// The API authenticates with bearer tokens rather than cookies, so a cross-origin page can only act with a token its user gave it
func (s *Server) middlewareCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Requests without an Origin header don't come from a browser page on another origin
		origin := r.Header.Get("Origin")
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}

		// A preflight asks whether the actual request may be sent, so it is answered here without reaching the routes
		preflight := r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
		allowed := s.cors.allows(origin)
		if preflight && !allowed {
			writeError(w, r, newAPIErrorf(http.StatusForbidden, CodeForbidden, "the origin '%s' is not allowed to call the API", origin))
			return
		}

		// Other requests from an origin that isn't allowed still run, without the headers that let the page read the response
		// Browsers also send an Origin header with same-origin POST requests, such as those of the embedded dashboard, which must keep working
		if !allowed {
			next.ServeHTTP(w, r)
			return
		}

		h := w.Header()
		h.Add("Vary", "Origin")
		h.Set("Access-Control-Allow-Origin", origin)
		if preflight {
			h.Add("Vary", "Access-Control-Request-Method")
			h.Add("Vary", "Access-Control-Request-Headers")
			h.Set("Access-Control-Allow-Methods", "GET, POST")
			h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			h.Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		h.Set("Access-Control-Expose-Headers", "X-Request-Id")
		next.ServeHTTP(w, r)
	})
}
