// Command alluvium serves fast per-collection exports of Atmosphere records
// folded from a Jetstream v2 instance, and builds that instance's repo list.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/flo-bit/alluvium/internal/discover"
	"github.com/flo-bit/alluvium/internal/fold"
	"github.com/flo-bit/alluvium/internal/identity"
	"github.com/flo-bit/alluvium/internal/labels"
	"github.com/flo-bit/alluvium/internal/safehttp"
	"github.com/flo-bit/alluvium/internal/server"
	"github.com/flo-bit/alluvium/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: alluvium <serve|discover> [flags]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(ctx, logger, os.Args[2:])
	case "discover":
		err = runDiscover(ctx, logger, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// matchesAny reports whether nsid matches an exact name or "prefix.*".
func matchesAny(patterns []string, nsid string) bool {
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

func runServe(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", env("ALLUVIUM_ADDR", "127.0.0.1:8090"), "listen address")
	dataDir := fs.String("data-dir", env("ALLUVIUM_DATA_DIR", "./data"), "store directory")
	source := fs.String("jetstream", env("ALLUVIUM_JETSTREAM", "http://127.0.0.1:8080"), "Jetstream v2 host to fold")
	tokens := fs.String("tokens-file", env("ALLUVIUM_TOKENS_FILE", ""), `file of "name token" lines; when set, exports require a token`)
	contact := fs.String("contact", env("ALLUVIUM_CONTACT", ""), "contact for the User-Agent on identity and profile lookups")
	deny := fs.String("deny", env("ALLUVIUM_DENY_COLLECTIONS", "app.bsky.*,chat.bsky.*"), "collections the source does not carry (exact or prefix.*); their profiles are fetched from the PDS")
	slingshot := fs.String("slingshot", env("ALLUVIUM_SLINGSHOT", "https://slingshot.microcosm.blue"), "identity resolver; empty to resolve directly")
	plc := fs.String("plc", env("ALLUVIUM_PLC", "https://plc.directory"), "PLC directory")
	labelers := fs.String("labelers", env("ALLUVIUM_LABELERS", ""), "comma-separated labeler DIDs to follow from the start; others are added the first time a request names them")
	_ = fs.Parse(args)

	st, err := store.Open(*dataDir)
	if errors.Is(err, store.ErrSchema) {
		// The store is derived data: drop it and refold from the source.
		logger.Warn("rebuilding store", "reason", err.Error())
		if err := os.RemoveAll(*dataDir); err != nil {
			return err
		}
		st, err = store.Open(*dataDir)
	}
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	folder := &fold.Folder{Host: *source, Store: st, Logger: logger.With("component", "fold")}
	userAgent := "alluvium/0.1"
	if *contact != "" {
		userAgent += " (" + *contact + ")"
	}
	// One client for every fetch whose host comes from a DID document or a
	// caller-supplied handle.
	fetch := safehttp.NewClient(8 * time.Second)
	denied := splitList(*deny)
	labelManager := &labels.Manager{
		Store: st, Logger: logger.With("component", "labels"), Client: fetch,
		PLC: strings.TrimRight(*plc, "/"), UserAgent: userAgent,
		// Account labels always matter; record labels only for collections
		// the store holds.
		Keep: func(subject string) bool {
			rest, isRecord := strings.CutPrefix(subject, "at://")
			if !isRecord {
				return strings.HasPrefix(subject, "did:")
			}
			parts := strings.Split(rest, "/")
			return len(parts) < 2 || !matchesAny(denied, parts[1])
		},
	}
	srv := &server.Server{
		Labels: labelManager, LabelStatus: labelManager.Status,
		Profiles: &server.Profiles{
			Identity: &identity.Resolver{
				Slingshot: strings.TrimRight(*slingshot, "/"), PLC: strings.TrimRight(*plc, "/"),
				UserAgent: userAgent, Client: fetch,
			},
			Client: fetch, UserAgent: userAgent,
			Held: func(collection string) bool { return !matchesAny(denied, collection) },
		},
		Store: st, Logger: logger.With("component", "http"), TokensFile: *tokens,
		FoldStats: func() (uint64, uint64) { return folder.Events(), folder.Errors() },
	}
	httpServer := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}

	errc := make(chan error, 3)
	go func() { errc <- folder.Run(ctx) }()
	go func() {
		if err := labelManager.Run(ctx, splitList(*labelers)); !errors.Is(err, context.Canceled) {
			errc <- err
		}
	}()
	go func() {
		logger.Info("listening", "addr", *addr)
		errc <- httpServer.ListenAndServe()
	}()

	err = <-errc
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runDiscover(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("discover", flag.ExitOnError)
	out := fs.String("out", env("ALLUVIUM_REPO_LIST", "./repos.txt"), "repo list to write (existing entries are kept)")
	contact := fs.String("contact", env("ALLUVIUM_CONTACT", ""), "contact for the User-Agent: an email address or Atmosphere handle (required)")
	statusURLs := fs.String("status-urls", env("ALLUVIUM_STATUS_URLS", "https://jetstream.us-east.bsky.network/status?tab=collections"), "comma-separated Jetstream status pages listing collections")
	nsidsFile := fs.String("nsids-file", env("ALLUVIUM_NSIDS_FILE", ""), "optional file of extra collection names")
	deny := fs.String("deny", env("ALLUVIUM_DENY_COLLECTIONS", "app.bsky.*,chat.bsky.*"), "collections to skip (exact or prefix.*)")
	keep := fs.String("keep", env("ALLUVIUM_KEEP_COLLECTIONS", ""), "exceptions to -deny")
	lightrail := fs.String("lightrail", env("ALLUVIUM_LIGHTRAIL", "https://lightrail.microcosm.blue"), "collection directory accepting 32 collections per request; empty to skip")
	relay := fs.String("relay", env("ALLUVIUM_RELAY", "https://bsky.network"), "relay collection directory; empty to skip")
	slingshot := fs.String("slingshot", env("ALLUVIUM_SLINGSHOT", "https://slingshot.microcosm.blue"), "identity resolver; empty to use PLC only")
	plc := fs.String("plc", env("ALLUVIUM_PLC", "https://plc.directory"), "PLC directory")
	workers := fs.Int("workers", 8, "concurrent directory requests")
	_ = fs.Parse(args)

	if *contact == "" {
		return errors.New("discover: -contact is required; the services ask for one in the User-Agent")
	}
	return discover.Run(ctx, discover.Config{
		StatusURLs: splitList(*statusURLs), NSIDsFile: *nsidsFile,
		Deny: splitList(*deny), Keep: splitList(*keep),
		Lightrail: strings.TrimRight(*lightrail, "/"), Relay: strings.TrimRight(*relay, "/"),
		Slingshot: strings.TrimRight(*slingshot, "/"), PLC: strings.TrimRight(*plc, "/"),
		Out: *out, UserAgent: "alluvium/0.1 (" + *contact + ")",
		Workers: *workers, Logger: logger.With("component", "discover"),
	})
}
