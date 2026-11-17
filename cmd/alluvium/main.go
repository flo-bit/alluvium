// Command alluvium serves fast per-collection exports of Atmosphere
// records. It builds snapshots of the current records from a Jetstream v2
// instance, serves exports from them, and builds that instance's repo list.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/flo-bit/alluvium/internal/build"
	"github.com/flo-bit/alluvium/internal/discover"
	"github.com/flo-bit/alluvium/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: alluvium <serve|build|discover> [flags]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(ctx, logger, os.Args[2:])
	case "build":
		err = runBuild(ctx, logger, os.Args[2:])
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

func envDuration(key string, fallback time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return d
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

func runServe(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", env("ALLUVIUM_ADDR", "127.0.0.1:8090"), "listen address")
	snapshots := fs.String("snapshots", env("ALLUVIUM_SNAPSHOTS", "./snapshots"), "snapshots directory, written by alluvium build")
	tokens := fs.String("tokens-file", env("ALLUVIUM_TOKENS_FILE", ""), `file of "name token" lines; when set, exports require a token`)
	maxAge := fs.Duration("max-age", envDuration("ALLUVIUM_MAX_AGE", 3*time.Hour), "age of the snapshot's last event beyond which /alluvium/health fails")
	debugAddr := fs.String("debug-addr", env("ALLUVIUM_DEBUG_ADDR", ""), "listen address for net/http/pprof; keep it on localhost")
	_ = fs.Parse(args)

	srv := &server.Server{Root: *snapshots, Logger: logger.With("component", "http"), TokensFile: *tokens, MaxAge: *maxAge}
	httpServer := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}

	debug(*debugAddr, logger)
	errc := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", *addr, "snapshots", *snapshots)
		errc <- httpServer.ListenAndServe()
	}()

	var err error
	select {
	case err = <-errc:
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// debug serves net/http/pprof on addr, if set.
func debug(addr string, logger *slog.Logger) {
	if addr == "" {
		return
	}
	go func() {
		// http.DefaultServeMux carries the pprof handlers.
		if err := http.ListenAndServe(addr, nil); err != nil {
			logger.Warn("debug listener", "err", err)
		}
	}()
}

func runBuild(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("build", flag.ExitOnError)
	snapshots := fs.String("snapshots", env("ALLUVIUM_SNAPSHOTS", "./snapshots"), "snapshots directory")
	source := fs.String("jetstream", env("ALLUVIUM_JETSTREAM", "http://127.0.0.1:8080"), "Jetstream v2 host to read events from")
	through := fs.Uint64("through", 0, "last sequence number to include; by default every event witnessed before the build started")
	deny := fs.String("deny", env("ALLUVIUM_DENY_COLLECTIONS", "app.bsky.*,chat.bsky.*"), "collections the source does not carry (exact or prefix.*), published so clients know")
	sortMB := fs.Int("sort-memory-mb", 128, "events held in memory before they are spilled to disk, in MiB")
	downloads := fs.Int("download-concurrency", 1, "parallel archive downloads; each holds a segment (about 270 MB) in memory")
	idle := fs.Duration("idle-timeout", 5*time.Minute, "build from what was read when the source delivers nothing for this long")
	debugAddr := fs.String("debug-addr", env("ALLUVIUM_BUILD_DEBUG_ADDR", ""), "listen address for net/http/pprof; keep it on localhost")
	_ = fs.Parse(args)
	debug(*debugAddr, logger)

	logger = logger.With("component", "build")
	report, err := build.Run(ctx, build.Config{
		Root:   *snapshots,
		Source: build.Jetstream(*source, *downloads, logger), Through: *through,
		Excludes: splitList(*deny), SortMemory: *sortMB << 20, IdleTimeout: *idle,
		Logger: logger,
	})
	if report != nil {
		logger.Info("build finished", "report", report)
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
