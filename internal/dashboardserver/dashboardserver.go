// Package dashboardserver serves the compiled management dashboard, either next to the management API or on its own
package dashboardserver

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
)

// Mode decides where the dashboard sends its API requests
type Mode string

const (
	// ModeEmbedded serves the dashboard next to the management API, which it calls on its own origin
	ModeEmbedded Mode = "embedded"
	// ModeStandalone serves the dashboard on its own, and it calls the management API endpoints its users add
	ModeStandalone Mode = "standalone"
)

var (
	// ErrNotFound is returned by Serve for a path that is neither a file nor one of the dashboard's routes
	ErrNotFound = errors.New("no such file")
	// ErrMethodNotAllowed is returned by Serve for a method other than GET and HEAD, after it set the Allow header
	ErrMethodNotAllowed = errors.New("method not allowed, use GET, HEAD")
)

const indexFile = "index.html"

// modeMeta finds the tag in the page that tells the dashboard its mode
// The page's Content-Security-Policy forbids inline scripts, so the mode travels in a meta tag that the server rewrites
var modeMeta = regexp.MustCompile(`<meta name="francis-dashboard-mode" content="[a-z]*"`)

// isCompressible reports whether a file with the extension is text, which is worth compressing
func isCompressible(ext string) bool {
	switch ext {
	case ".css", ".html", ".js", ".json", ".map", ".svg", ".txt":
		return true
	default:
		return false
	}
}

// pageCSP returns the Content-Security-Policy of the dashboard's page
// The page only loads its own files and can't be framed
// Embedded, it only talks to its own origin, while standalone it must reach whichever endpoints its users add
func pageCSP(mode Mode) string {
	connect := "'self'"
	if mode == ModeStandalone {
		connect = "'self' http: https:"
	}

	return "default-src 'none'; " +
		"script-src 'self'; " +
		"style-src 'self'; " +
		"img-src 'self'; " +
		"font-src 'self'; " +
		"connect-src " + connect + "; " +
		"frame-ancestors 'none'; " +
		"base-uri 'none'; " +
		"form-action 'none'"
}

// Options contains the options for New
type Options struct {
	// Files holds the compiled dashboard, with index.html at its root
	Files fs.FS
	// Mode decides where the dashboard sends its API requests
	Mode Mode
}

// Dashboard serves the compiled dashboard from memory
type Dashboard struct {
	files map[string]*file
}

// file is one of the dashboard's files, with its response headers prepared once
type file struct {
	data []byte
	// gzipped is the gzip-compressed data, nil when compressing does not make the file smaller
	gzipped      []byte
	etag         string
	contentType  string
	cacheControl string
	// csp is the Content-Security-Policy of the page, and empty for every other file
	csp string
}

// New loads every file of the compiled dashboard
func New(opts Options) (*Dashboard, error) {
	if opts.Files == nil {
		return nil, errors.New("the dashboard files are required")
	}
	if opts.Mode != ModeEmbedded && opts.Mode != ModeStandalone {
		return nil, fmt.Errorf("invalid dashboard mode '%s'", opts.Mode)
	}

	d := &Dashboard{
		files: map[string]*file{},
	}

	err := fs.WalkDir(opts.Files, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Hidden files, such as the placeholder that keeps the output directory in the source tree, are not part of the dashboard
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			return nil
		}

		data, err := fs.ReadFile(opts.Files, name)
		if err != nil {
			return fmt.Errorf("failed to read dashboard file '%s': %w", name, err)
		}

		// The page tells the dashboard its mode
		var csp string
		if name == indexFile {
			if !modeMeta.Match(data) {
				return errors.New("the dashboard page has no francis-dashboard-mode meta tag")
			}
			data = modeMeta.ReplaceAllLiteral(data, []byte(`<meta name="francis-dashboard-mode" content="`+string(opts.Mode)+`"`))
			csp = pageCSP(opts.Mode)
		}

		f, err := newFile(name, data, csp)
		if err != nil {
			return err
		}
		d.files[name] = f
		return nil
	})
	if err != nil {
		return nil, err
	}

	_, ok := d.files[indexFile]
	if !ok {
		return nil, errors.New("the dashboard has no " + indexFile + " file")
	}

	return d, nil
}

func newFile(name string, data []byte, csp string) (*file, error) {
	ext := strings.ToLower(path.Ext(name))
	sum := sha256.Sum256(data)

	f := &file{
		data:        data,
		etag:        base64.RawURLEncoding.EncodeToString(sum[:18]),
		contentType: mime.TypeByExtension(ext),
		csp:         csp,
	}

	// Ensure we have a default content type if missing
	if f.contentType == "" {
		f.contentType = "application/octet-stream"
	}

	// Vite puts the hashed bundles under assets/, so their content never changes for a given name
	// Every other file, the page above all, is revalidated so a new build is picked up right away
	if strings.HasPrefix(name, "assets/") {
		f.cacheControl = "public, max-age=31536000, immutable"
	} else {
		f.cacheControl = "no-cache"
	}

	// Compress text files once, rather than on every request
	if isCompressible(ext) {
		var buf bytes.Buffer
		gz, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
		if err != nil {
			return nil, err
		}
		_, err = gz.Write(data)
		if err != nil {
			return nil, fmt.Errorf("failed to compress dashboard file '%s': %w", name, err)
		}
		err = gz.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to compress dashboard file '%s': %w", name, err)
		}

		if buf.Len() < len(data) {
			f.gzipped = buf.Bytes()
		}
	}

	return f, nil
}

// Serve responds with a file of the dashboard, or with its page for the dashboard's own routes
// It returns ErrNotFound or ErrMethodNotAllowed without writing a response, so the caller can report them in its own format
func (d *Dashboard) Serve(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		return ErrMethodNotAllowed
	}

	// The cleaned path can't escape the root, and is only ever a key in the map of files
	name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if name == "" {
		name = indexFile
	}

	// Any other path is one of the dashboard's routes, which the page resolves in the browser
	// Only navigations get the page, so a missing script, stylesheet, or image is a 404 rather than an HTML document
	f, ok := d.files[name]
	if !ok {
		if strings.HasPrefix(name, "assets/") || !acceptsHTML(r) {
			return ErrNotFound
		}
		f = d.files[indexFile]
	}

	f.write(w, r)
	return nil
}

// ServeHTTP serves the dashboard on its own, reporting errors as plain text
func (d *Dashboard) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := d.Serve(w, r)
	switch {
	case errors.Is(err, ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, ErrMethodNotAllowed):
		http.Error(w, err.Error(), http.StatusMethodNotAllowed)
	}
}

// write sends the file, compressed when the client accepts it, or a 304 when the client's copy is current
func (f *file) write(w http.ResponseWriter, r *http.Request) {
	h := w.Header()

	// Pick the representation, each with its own ETag
	body := f.data
	etag := `"` + f.etag + `"`
	if f.gzipped != nil {
		h.Set("Vary", "Accept-Encoding")
		if acceptsGzip(r) {
			body = f.gzipped
			etag = `"` + f.etag + `-gz"`
			h.Set("Content-Encoding", "gzip")
		}
	}

	h.Set("Content-Type", f.contentType)
	h.Set("Cache-Control", f.cacheControl)
	h.Set("ETag", etag)
	h.Set("X-Content-Type-Options", "nosniff")
	if f.csp != "" {
		h.Set("Content-Security-Policy", f.csp)
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		h.Set("X-Robots-Tag", "noindex, nofollow")
	}

	if etagMatches(r.Header.Get("If-None-Match"), etag) {
		h.Del("Content-Type")
		h.Del("Content-Encoding")
		w.WriteHeader(http.StatusNotModified)
		return
	}

	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

// acceptsHTML reports whether the request is a browser navigation, which lists text/html in its Accept header
func acceptsHTML(r *http.Request) bool {
	for _, v := range r.Header.Values("Accept") {
		for part := range strings.SplitSeq(v, ",") {
			mt, _, _ := strings.Cut(part, ";")
			if strings.EqualFold(strings.TrimSpace(mt), "text/html") {
				return true
			}
		}
	}
	return false
}

// acceptsGzip reports whether the client accepts gzip-compressed responses
// A coding listed with a quality of zero is refused
func acceptsGzip(r *http.Request) bool {
	for _, v := range r.Header.Values("Accept-Encoding") {
		for part := range strings.SplitSeq(v, ",") {
			coding, params, _ := strings.Cut(part, ";")
			if !strings.EqualFold(strings.TrimSpace(coding), "gzip") {
				continue
			}

			q, ok := strings.CutPrefix(strings.ReplaceAll(params, " ", ""), "q=")
			if ok {
				qv, err := strconv.ParseFloat(q, 64)
				if err == nil && qv == 0 {
					return false
				}
			}
			return true
		}
	}
	return false
}

// etagMatches reports whether an If-None-Match header lists the ETag, using the weak comparison the header calls for
func etagMatches(header string, etag string) bool {
	if header == "" {
		return false
	}

	for part := range strings.SplitSeq(header, ",") {
		part = strings.TrimSpace(part)
		if part == "*" || strings.TrimPrefix(part, "W/") == etag {
			return true
		}
	}
	return false
}
