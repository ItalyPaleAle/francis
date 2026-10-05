// Command helm-repo writes the index of the Helm chart repository served at https://charts.gofrancis.dev
// The release workflow publishes every chart as an OCI artifact to the GitHub Container Registry, so the index is generated from what the registry holds and each entry points at its OCI reference
// Helm downloads a chart whose URL uses the oci:// scheme straight from the registry, so the site only serves the index and never the archives
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	ociManifestMediaType = "application/vnd.oci.image.manifest.v1+json"
	helmConfigMediaType  = "application/vnd.cncf.helm.config.v1+json"
	helmChartMediaType   = "application/vnd.cncf.helm.chart.content.v1.tar+gzip"

	// Annotation Helm sets on the manifest of every chart it pushes
	createdAnnotation = "org.opencontainers.image.created"
)

// Helm tags a chart with its version, replacing "+" with "_" because OCI tags cannot contain "+"
// Anything else in the repository, such as the "sha256-…" tags that hold attestations, is not a chart
var chartTagRegexp = regexp.MustCompile(`^v?[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(_[0-9A-Za-z.-]+)?$`)

// errNotChart is returned for a tag whose manifest does not describe a Helm chart
var errNotChart = errors.New("not a Helm chart")

// indexFile is the Helm repository index format
// Helm parses the index as YAML, and JSON is valid YAML, which is also what "helm repo index --json" writes
type indexFile struct {
	APIVersion string                      `json:"apiVersion"`
	Entries    map[string][]map[string]any `json:"entries"`
	Generated  string                      `json:"generated"`
}

// ociManifest is the subset of an OCI image manifest needed to locate a chart's metadata and archive
type ociManifest struct {
	Config      ociDescriptor     `json:"config"`
	Layers      []ociDescriptor   `json:"layers"`
	Annotations map[string]string `json:"annotations"`
}

type ociDescriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
}

func main() {
	registry := flag.String("registry", "https://ghcr.io", "Base URL of the OCI registry that holds the charts")
	repository := flag.String("repository", "italypaleale/charts/francis", "Repository of the charts in the registry")
	out := flag.String("out", filepath.Join("public", "index.yaml"), "Path where the index is written")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	err := run(ctx, *registry, *repository, *out)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "helm-repo: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, registryURL string, repository string, out string) error {
	client, err := newRegistryClient(registryURL, repository)
	if err != nil {
		return err
	}

	// Collect every chart version in the registry
	index, err := buildIndex(ctx, client, time.Now())
	if err != nil {
		return err
	}

	// An empty index means the registry lost the charts or the lookup is broken, and publishing it would hide every chart from users
	count := 0
	for _, versions := range index.Entries {
		count += len(versions)
	}
	if count == 0 {
		return fmt.Errorf("no charts found in %s/%s", client.host(), repository)
	}

	// Write the index into the folder Vercel serves
	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode the index: %w", err)
	}
	data = append(data, '\n')

	err = os.MkdirAll(filepath.Dir(out), 0o755)
	if err != nil {
		return fmt.Errorf("failed to create the directory for %s: %w", out, err)
	}
	err = os.WriteFile(out, data, 0o644) //nolint:gosec // G306: the index is published on the website
	if err != nil {
		return fmt.Errorf("failed to write %s: %w", out, err)
	}

	fmt.Fprintf(os.Stdout, "helm-repo: wrote %d chart versions to %s\n", count, out)
	return nil
}

// buildIndex lists the charts in the repository and reads the metadata of each one into an index entry
func buildIndex(ctx context.Context, client *registryClient, now time.Time) (*indexFile, error) {
	tags, err := client.listTags(ctx)
	if err != nil {
		return nil, err
	}

	// Read every chart version, skipping tags that hold something other than a chart
	entries := make(map[string][]map[string]any)
	for _, tag := range tags {
		if !chartTagRegexp.MatchString(tag) {
			continue
		}

		entry, err := chartVersion(ctx, client, tag)
		if errors.Is(err, errNotChart) {
			fmt.Fprintf(os.Stderr, "helm-repo: skipping tag %s: %v\n", tag, err)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read the chart tagged %s: %w", tag, err)
		}

		name, _ := entry["name"].(string)
		entries[name] = append(entries[name], entry)
	}

	// Helm sorts the versions itself when it loads the index, so newest first only makes the file easier to read
	for _, versions := range entries {
		sort.SliceStable(versions, func(i, j int) bool {
			return createdTime(versions[i]).After(createdTime(versions[j]))
		})
	}

	return &indexFile{
		APIVersion: "v1",
		Entries:    entries,
		Generated:  now.UTC().Format(time.RFC3339),
	}, nil
}

// chartVersion builds the index entry for the chart pushed with the given tag
func chartVersion(ctx context.Context, client *registryClient, tag string) (map[string]any, error) {
	// The manifest points at the chart's metadata and at its packaged archive
	var manifest ociManifest
	err := client.getJSON(ctx, "manifests/"+tag, ociManifestMediaType, &manifest)
	if err != nil {
		return nil, err
	}
	if manifest.Config.MediaType != helmConfigMediaType {
		return nil, fmt.Errorf("%w: config has media type %q", errNotChart, manifest.Config.MediaType)
	}

	var chartLayer *ociDescriptor
	for i := range manifest.Layers {
		if manifest.Layers[i].MediaType == helmChartMediaType {
			chartLayer = &manifest.Layers[i]
			break
		}
	}
	if chartLayer == nil {
		return nil, fmt.Errorf("%w: no layer has media type %q", errNotChart, helmChartMediaType)
	}

	// Index entries carry the SHA-256 of the packaged archive, and the chart layer is that archive byte for byte
	digest, ok := strings.CutPrefix(chartLayer.Digest, "sha256:")
	if !ok {
		return nil, fmt.Errorf("chart archive has unsupported digest %q", chartLayer.Digest)
	}

	// The config blob is the chart's Chart.yaml as JSON, which is exactly the metadata an index entry carries
	// Decoding it into a map keeps every field, including ones added by Helm versions newer than this command
	var entry map[string]any
	err = client.getJSON(ctx, "blobs/"+manifest.Config.Digest, "", &entry)
	if err != nil {
		return nil, err
	}
	name, _ := entry["name"].(string)
	if name == "" {
		return nil, errors.New("chart metadata has no name")
	}

	entry["urls"] = []string{"oci://" + client.host() + "/" + client.repository + ":" + tag}
	entry["digest"] = digest
	created := manifest.Annotations[createdAnnotation]
	if created != "" {
		entry["created"] = created
	}

	return entry, nil
}

// createdTime returns when an index entry was published, or the zero time if it does not say
func createdTime(entry map[string]any) time.Time {
	created, _ := entry["created"].(string)
	t, err := time.Parse(time.RFC3339, created)
	if err != nil {
		return time.Time{}
	}
	return t
}
