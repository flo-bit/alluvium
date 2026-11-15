// Package discover builds the list of repos that hold at least one record
// in a collection alluvium keeps, with the PDS host of each, for the
// Jetstream fork's --backfill-repo-list.
package discover

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

type Config struct {
	// StatusURLs are Jetstream status pages listing every collection the
	// instance has seen (?tab=collections).
	StatusURLs []string
	// NSIDsFile optionally adds collection names, one per line.
	NSIDsFile string
	// Deny and Keep use the Jetstream fork's filter syntax: exact NSIDs or
	// "prefix.*". A collection is enumerated unless denied and not kept.
	Deny, Keep []string
	// Lightrail and Relay are listReposByCollection hosts. Either may be
	// empty; results are unioned.
	Lightrail, Relay string
	// Slingshot resolves a DID to its PDS; PLC is the fallback.
	Slingshot, PLC string
	// Out is the repo list ("did host" per line). Existing entries are kept.
	Out string
	// UserAgent identifies this deployment and a contact to the services.
	UserAgent string
	Workers   int
	Logger    *slog.Logger

	client *http.Client
}

var nsidRe = regexp.MustCompile(`^[a-zA-Z]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?){2,}$`)

func matches(patterns []string, nsid string) bool {
	for _, p := range patterns {
		if strings.HasSuffix(p, ".*") {
			if strings.HasPrefix(nsid, strings.TrimSuffix(p, "*")) {
				return true
			}
		} else if p == nsid {
			return true
		}
	}
	return false
}

func (c *Config) wanted(nsid string) bool {
	return nsidRe.MatchString(nsid) && (!matches(c.Deny, nsid) || matches(c.Keep, nsid))
}

func (c *Config) get(ctx context.Context, rawURL string) ([]byte, int, error) {
	var lastErr error
	for attempt := range 6 {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(time.Duration(attempt) * 2 * time.Second):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("User-Agent", c.UserAgent)
		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("%s: http %d", rawURL, resp.StatusCode)
			continue
		}
		return body, resp.StatusCode, nil
	}
	return nil, 0, lastErr
}

var rowRe = regexp.MustCompile(`(?s)<tr[^>]*>\s*<td[^>]*>(.*?)</td>`)
var tagRe = regexp.MustCompile(`<[^>]+>`)

// collections gathers candidate NSIDs from the status pages and NSIDsFile.
func (c *Config) collections(ctx context.Context) ([]string, error) {
	set := map[string]struct{}{}
	for _, u := range c.StatusURLs {
		body, status, err := c.get(ctx, u)
		if err != nil || status != http.StatusOK {
			return nil, fmt.Errorf("discover: status page %s: http %d: %w", u, status, err)
		}
		n := 0
		for _, m := range rowRe.FindAllSubmatch(body, -1) {
			nsid := strings.TrimSpace(html.UnescapeString(tagRe.ReplaceAllString(string(m[1]), "")))
			if c.wanted(nsid) {
				set[nsid] = struct{}{}
				n++
			}
		}
		if n == 0 {
			return nil, fmt.Errorf("discover: status page %s listed no collections; its layout may have changed", u)
		}
		c.Logger.Info("read collections", "source", u, "kept", n)
	}
	if c.NSIDsFile != "" {
		f, err := os.Open(c.NSIDsFile)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if nsid := strings.TrimSpace(sc.Text()); c.wanted(nsid) {
				set[nsid] = struct{}{}
			}
		}
		_ = f.Close()
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	out := make([]string, 0, len(set))
	for nsid := range set {
		out = append(out, nsid)
	}
	sort.Strings(out)
	return out, nil
}

type listPage struct {
	Repos  []struct{ DID string } `json:"repos"`
	Cursor string                 `json:"cursor"`
}

// listRepos pages listReposByCollection for one group of collections.
func (c *Config) listRepos(ctx context.Context, host string, group []string, limit int, add func(string)) error {
	cursor := ""
	for {
		q := url.Values{"collection": group, "limit": {fmt.Sprint(limit)}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		body, status, err := c.get(ctx, host+"/xrpc/com.atproto.sync.listReposByCollection?"+q.Encode())
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("%s: http %d: %.200s", host, status, body)
		}
		var page listPage
		if err := json.Unmarshal(body, &page); err != nil {
			return fmt.Errorf("%s: %w", host, err)
		}
		for _, r := range page.Repos {
			add(r.DID)
		}
		if page.Cursor == "" || len(page.Repos) == 0 {
			return nil
		}
		cursor = page.Cursor
	}
}

// enumerate unions the repos of every group across one directory.
func (c *Config) enumerate(ctx context.Context, host string, nsids []string, perRequest, limit int, dids map[string]struct{}, mu *sync.Mutex) (failed int) {
	var groups [][]string
	for i := 0; i < len(nsids); i += perRequest {
		groups = append(groups, nsids[i:min(i+perRequest, len(nsids))])
	}
	work := make(chan []string)
	var wg sync.WaitGroup
	var failedMu sync.Mutex
	for range c.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for group := range work {
				err := c.listRepos(ctx, host, group, limit, func(did string) {
					mu.Lock()
					dids[did] = struct{}{}
					mu.Unlock()
				})
				if err != nil {
					failedMu.Lock()
					failed++
					failedMu.Unlock()
					c.Logger.Warn("directory request failed", "host", host, "first", group[0], "err", err)
				}
			}
		}()
	}
	for i, group := range groups {
		if ctx.Err() != nil {
			break
		}
		work <- group
		if i > 0 && i%500 == 0 {
			mu.Lock()
			n := len(dids)
			mu.Unlock()
			c.Logger.Info("enumerating", "host", host, "requests", i, "of", len(groups), "repos", n)
		}
	}
	close(work)
	wg.Wait()
	return failed
}

func hostOf(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return ""
	}
	return strings.ToLower(u.Host)
}

// resolve returns the PDS host of did, or "" when it cannot be determined.
func (c *Config) resolve(ctx context.Context, did string) string {
	if c.Slingshot != "" {
		body, status, err := c.get(ctx, c.Slingshot+"/xrpc/com.bad-example.identity.resolveMiniDoc?identifier="+url.QueryEscape(did))
		if err == nil && status == http.StatusOK {
			var doc struct{ PDS string }
			if json.Unmarshal(body, &doc) == nil {
				if h := hostOf(doc.PDS); h != "" {
					return h
				}
			}
		}
	}
	docURL := ""
	switch {
	case strings.HasPrefix(did, "did:plc:") && c.PLC != "":
		docURL = c.PLC + "/" + did
	case strings.HasPrefix(did, "did:web:"):
		docURL = "https://" + strings.ReplaceAll(strings.TrimPrefix(did, "did:web:"), ":", "/") + "/.well-known/did.json"
	default:
		return ""
	}
	body, status, err := c.get(ctx, docURL)
	if err != nil || status != http.StatusOK {
		return ""
	}
	var doc struct {
		Service []struct {
			ID              string `json:"id"`
			ServiceEndpoint any    `json:"serviceEndpoint"`
		} `json:"service"`
	}
	if json.Unmarshal(body, &doc) != nil {
		return ""
	}
	for _, svc := range doc.Service {
		if strings.HasSuffix(svc.ID, "#atproto_pds") {
			if endpoint, ok := svc.ServiceEndpoint.(string); ok {
				return hostOf(endpoint)
			}
		}
	}
	return ""
}

// readList loads an existing repo list: did -> host ("" when unknown).
func readList(path string) (map[string]string, error) {
	out := map[string]string{}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		host := ""
		if len(fields) > 1 {
			host = fields[1]
		}
		out[fields[0]] = host
	}
	return out, sc.Err()
}

func writeList(path string, list map[string]string) error {
	dids := make([]string, 0, len(list))
	for did := range list {
		dids = append(dids, did)
	}
	sort.Strings(dids)
	tmp, err := os.CreateTemp(filepath.Dir(path), ".repos-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	w := bufio.NewWriter(tmp)
	for _, did := range dids {
		if host := list[did]; host != "" {
			fmt.Fprintf(w, "%s %s\n", did, host)
		} else {
			fmt.Fprintln(w, did)
		}
	}
	if err := errors.Join(w.Flush(), tmp.Sync(), tmp.Chmod(0o644), tmp.Close()); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// Run enumerates, resolves new repos, and rewrites Out. The list only
// grows: Jetstream ignores entries it already knows, and a repo that drops
// out of a directory may still hold records.
func Run(ctx context.Context, c Config) error {
	if c.Workers <= 0 {
		c.Workers = 8
	}
	c.client = &http.Client{Timeout: 60 * time.Second}

	nsids, err := c.collections(ctx)
	if err != nil {
		return err
	}
	if len(nsids) == 0 {
		return errors.New("discover: no collections to enumerate")
	}
	c.Logger.Info("collections to enumerate", "count", len(nsids))

	dids := map[string]struct{}{}
	var mu sync.Mutex
	failed := 0
	if c.Lightrail != "" {
		failed += c.enumerate(ctx, c.Lightrail, nsids, 32, 10000, dids, &mu)
		c.Logger.Info("enumerated", "host", c.Lightrail, "repos", len(dids))
	}
	if c.Relay != "" {
		failed += c.enumerate(ctx, c.Relay, nsids, 1, 2000, dids, &mu)
		c.Logger.Info("enumerated", "host", c.Relay, "repos", len(dids))
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	list, err := readList(c.Out)
	if err != nil {
		return err
	}
	var unresolved []string
	for did := range dids {
		if host, known := list[did]; !known || host == "" {
			unresolved = append(unresolved, did)
		}
	}
	sort.Strings(unresolved)
	c.Logger.Info("resolving PDS hosts", "repos", len(unresolved), "already_listed", len(list))

	work := make(chan string)
	var wg sync.WaitGroup
	resolved, missing := 0, 0
	for range c.Workers * 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for did := range work {
				host := c.resolve(ctx, did)
				mu.Lock()
				list[did] = host
				if host == "" {
					missing++
				} else {
					resolved++
				}
				done := resolved + missing
				mu.Unlock()
				if done%10000 == 0 {
					c.Logger.Info("resolving", "done", done, "of", len(unresolved))
				}
			}
		}()
	}
	for _, did := range unresolved {
		if ctx.Err() != nil {
			break
		}
		work <- did
	}
	close(work)
	wg.Wait()

	if err := writeList(c.Out, list); err != nil {
		return err
	}
	c.Logger.Info("wrote repo list", "path", c.Out, "repos", len(list),
		"new", len(unresolved), "without_host", missing, "failed_directory_requests", failed)
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
