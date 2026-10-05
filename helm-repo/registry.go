package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	// Each attempt at a request is cut off after this long
	requestTimeout = 30 * time.Second

	// Requests that fail with a network error or a server-side status are tried this many times in total
	maxAttempts = 3

	// Responses are small JSON documents, so anything larger than this is a mistake
	maxBodySize = 16 << 20
)

var (
	// Matches the parameters of a WWW-Authenticate challenge, such as realm="https://ghcr.io/token"
	challengeParamRegexp = regexp.MustCompile(`([a-zA-Z]+)="([^"]*)"`)

	// Matches the next page in a Link header, such as </v2/a/b/tags/list?last=x&n=100>; rel="next"
	nextLinkRegexp = regexp.MustCompile(`<([^>]+)>\s*;\s*rel="?next"?`)
)

// registryClient reads a single repository from an OCI registry with anonymous pull access
type registryClient struct {
	baseURL    *url.URL
	repository string
	httpClient *http.Client
	retryDelay time.Duration
	token      string
}

type registryResponse struct {
	status int
	header http.Header
	body   []byte
}

func newRegistryClient(registryURL string, repository string) (*registryClient, error) {
	u, err := url.Parse(registryURL)
	if err != nil {
		return nil, fmt.Errorf("invalid registry URL %q: %w", registryURL, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid registry URL %q: missing host", registryURL)
	}

	return &registryClient{
		baseURL:    u,
		repository: strings.Trim(repository, "/"),
		httpClient: &http.Client{Timeout: requestTimeout},
		retryDelay: time.Second,
	}, nil
}

// host returns the registry as an OCI reference names it
func (c *registryClient) host() string {
	return c.baseURL.Host
}

// listTags returns every tag in the repository, following the registry's pagination
func (c *registryClient) listTags(ctx context.Context) ([]string, error) {
	var tags []string
	next := "/v2/" + c.repository + "/tags/list?n=1000"
	for next != "" {
		res, err := c.get(ctx, next, "application/json")
		if err != nil {
			return nil, err
		}

		var page struct {
			Tags []string `json:"tags"`
		}
		err = json.Unmarshal(res.body, &page)
		if err != nil {
			return nil, fmt.Errorf("failed to decode the tag list: %w", err)
		}
		tags = append(tags, page.Tags...)

		next = ""
		match := nextLinkRegexp.FindStringSubmatch(res.header.Get("Link"))
		if match != nil {
			next = match[1]
		}
	}

	return tags, nil
}

// getJSON fetches a manifest or blob from the repository and decodes it
func (c *registryClient) getJSON(ctx context.Context, path string, accept string, out any) error {
	res, err := c.get(ctx, "/v2/"+c.repository+"/"+path, accept)
	if err != nil {
		return err
	}

	err = json.Unmarshal(res.body, out)
	if err != nil {
		return fmt.Errorf("failed to decode %s: %w", path, err)
	}
	return nil
}

// get fetches a path from the registry and fails unless the response is a 200
func (c *registryClient) get(ctx context.Context, ref string, accept string) (*registryResponse, error) {
	u, err := c.baseURL.Parse(ref)
	if err != nil {
		return nil, fmt.Errorf("invalid registry path %q: %w", ref, err)
	}

	res, err := c.send(ctx, u, accept)
	if err != nil {
		return nil, err
	}

	// Even public repositories need a token, and the registry's challenge says where to get one
	if res.status == http.StatusUnauthorized && c.token == "" {
		err = c.authenticate(ctx, res.header.Get("WWW-Authenticate"))
		if err != nil {
			return nil, err
		}

		res, err = c.send(ctx, u, accept)
		if err != nil {
			return nil, err
		}
	}

	if res.status != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned status %d: %s", u.Redacted(), res.status, truncate(res.body))
	}
	return res, nil
}

// authenticate gets an anonymous pull token from the authorization server named in a Bearer challenge
func (c *registryClient) authenticate(ctx context.Context, challenge string) error {
	scheme, params, _ := strings.Cut(challenge, " ")
	if !strings.EqualFold(scheme, "Bearer") {
		return fmt.Errorf("registry requires unsupported authentication %q", challenge)
	}

	values := make(map[string]string)
	for _, match := range challengeParamRegexp.FindAllStringSubmatch(params, -1) {
		values[strings.ToLower(match[1])] = match[2]
	}
	if values["realm"] == "" {
		return fmt.Errorf("registry challenge %q has no realm", challenge)
	}

	// Request pull access to this repository only, since that is all the index needs
	tokenURL, err := url.Parse(values["realm"])
	if err != nil {
		return fmt.Errorf("registry challenge has invalid realm %q: %w", values["realm"], err)
	}
	query := tokenURL.Query()
	if values["service"] != "" {
		query.Set("service", values["service"])
	}
	query.Set("scope", "repository:"+c.repository+":pull")
	tokenURL.RawQuery = query.Encode()

	res, err := c.send(ctx, tokenURL, "application/json")
	if err != nil {
		return err
	}
	if res.status != http.StatusOK {
		return fmt.Errorf("token request returned status %d: %s", res.status, truncate(res.body))
	}

	// The token spec allows either field name
	var token struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	err = json.Unmarshal(res.body, &token)
	if err != nil {
		return fmt.Errorf("failed to decode the token response: %w", err)
	}
	c.token = token.Token
	if c.token == "" {
		c.token = token.AccessToken
	}
	if c.token == "" {
		return fmt.Errorf("token response has no token: %s", truncate(res.body))
	}

	return nil
}

// send performs a GET, retrying network errors and server-side failures since a passing registry hiccup would otherwise fail the whole build
func (c *registryClient) send(ctx context.Context, u *url.URL, accept string) (*registryResponse, error) {
	var lastErr error
	for attempt := range maxAttempts {
		// Back off a little longer before each retry
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * c.retryDelay):
			}
		}

		res, err := c.sendOnce(ctx, u, accept)
		if err != nil {
			lastErr = err
			continue
		}
		if res.status >= http.StatusInternalServerError || res.status == http.StatusTooManyRequests {
			lastErr = fmt.Errorf("GET %s returned status %d: %s", u.Redacted(), res.status, truncate(res.body))
			continue
		}

		return res, nil
	}

	return nil, lastErr
}

func (c *registryClient) sendOnce(ctx context.Context, u *url.URL, accept string) (*registryResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	// Blobs redirect to storage on another host, and the HTTP client drops this header when it follows a redirect to a different domain
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s failed: %w", u.Redacted(), err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(io.LimitReader(res.Body, maxBodySize))
	if err != nil {
		return nil, fmt.Errorf("failed to read the response to GET %s: %w", u.Redacted(), err)
	}

	return &registryResponse{
		status: res.StatusCode,
		header: res.Header,
		body:   body,
	}, nil
}

// truncate shortens a response body so it can be included in an error message
func truncate(body []byte) string {
	const limit = 200
	s := strings.TrimSpace(string(body))
	if len(s) > limit {
		return s[:limit] + "…"
	}
	return s
}
