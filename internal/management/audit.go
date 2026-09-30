package management

import (
	"log/slog"
	"net/http"
	"strings"
)

// auditBase returns the attributes every audit record carries
func auditBase(r *http.Request, event string) []any {
	var suffix string
	caller := callerFromContext(r.Context())
	if caller != nil {
		suffix = caller.TokenSuffix
	}

	return []any{
		slog.String("event", event),
		slog.String("requestId", requestIDFromContext(r.Context())),
		slog.String("tokenSuffix", suffix),
		slog.String("remoteAddr", r.RemoteAddr),
	}
}

// auditRead records a read of sensitive data, such as actor state or workflow input and output
// The data itself is never logged
func (s *Server) auditRead(r *http.Request, event string, attrs ...any) {
	s.audit.InfoContext(r.Context(), "Management API sensitive read", append(auditBase(r, event), attrs...)...)
}

// auditAction records the outcome of an action
// Reasons are logged, but request payloads never are
func (s *Server) auditAction(r *http.Request, event string, apiErr *apiError, attrs ...any) {
	all := append(auditBase(r, event), attrs...)
	if apiErr != nil {
		all = append(all, slog.String("result", "failed"), slog.String("errorCode", apiErr.Code), slog.Int("status", apiErr.status))
		s.audit.WarnContext(r.Context(), "Management API action", all...)
		return
	}

	all = append(all, slog.String("result", "accepted"))
	s.audit.InfoContext(r.Context(), "Management API action", all...)
}

// withAuditReason appends the attributes that record an action's reason to attrs
// A reason longer than maxReasonLength is rejected, but the rejection is audited too, so the logged reason is cut to that length rather than letting a request write up to its whole body into the log
func withAuditReason(reason string, attrs ...any) []any {
	if len(reason) <= maxReasonLength {
		return append(attrs, slog.String("reason", reason))
	}

	// Cutting at a byte offset can split a character, which ToValidUTF8 drops
	return append(attrs,
		slog.String("reason", strings.ToValidUTF8(reason[:maxReasonLength], "")),
		slog.Bool("reasonTruncated", true),
	)
}
