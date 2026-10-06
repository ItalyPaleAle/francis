// Package dashboard embeds the compiled management dashboard, which the runtime serves on the management listener
// Build it with `make dashboard` before compiling the runtime; without it, the runtime serves only the management API
package dashboard

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var dist embed.FS

// Files returns the compiled dashboard, rooted at its index.html, or nil when this binary was built without it
func Files() fs.FS {
	files, err := fs.Sub(dist, "dist")
	if err != nil {
		return nil
	}

	_, err = fs.Stat(files, "index.html")
	if err != nil {
		return nil
	}

	return files
}
