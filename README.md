# Alluvium

Fast backfill for Atmosphere records outside the Bluesky app.

Ask for a collection and get every current record of it in one streamed
response, then follow the live stream from the cursor it ends with.

```sh
curl --compressed 'https://backfill.atmo.tools/export?collection=community.lexicon.calendar.event'
```

## How it works

- **Jetstream** (`../jetstream`, branch `alluvium`) is a fork of
  [Jetstream v2](https://github.com/bluesky-social/jetstream) with three
  small additions: an ingest filter that drops `app.bsky.*` and `chat.bsky.*`,
  backfill from a repo list instead of the whole network, and a configurable
  User-Agent. It verifies and archives the firehose, backfills listed repos
  from their PDS, and serves the stock Jetstream v2 API. Every full-repo
  download goes through one disk-backed path: repos over 64 MB are parsed
  from a temporary file, and a repo whose commit chain breaks is queued for
  re-download there instead of being resynced in memory.
- **`alluvium build`** runs every hour. It merges the previous snapshot with
  the Jetstream events since its cursor (deletes applied, deleted accounts
  purged, inactive accounts hidden) and publishes a new immutable snapshot:
  every current record, already rendered as export lines and compressed,
  sorted by collection.
- **`alluvium serve`** serves exports from the latest snapshot. It does not
  talk to Jetstream; an export is mostly a copy of file bytes.
- **`alluvium discover`** builds the repo list: it takes every collection
  name the official Jetstream instance has seen, asks Lightrail and the relay
  which repos hold them, and resolves each repo's PDS. Jetstream re-reads the
  list on every retry pass, so a daily run keeps coverage current.

An export is as old as its snapshot, up to an hour and a build. That costs a
consumer nothing but replay: the export names the cursor it is consistent
with, the consumer replays the live stream from there, and applying those
events on top is idempotent. Non-Bluesky traffic is about a million events
a day.

## API

`https://backfill.atmo.tools`

| Route | |
|---|---|
| `GET /export?collection=A&collection=B[&after=<uri>]` | Current records of up to 100 collections |
| `GET /collections` | The snapshot's head and every collection with its record count |
| `GET /alluvium/head` | The snapshot being served: cursor, its time, excluded collections |
| `GET /alluvium/health` | 200 while the snapshot's last event is under three hours old |
| `GET /alluvium/status` | Snapshot totals, the last build's report, export usage |
| `/subscribe`, `/xrpc/network.bsky.jetstream.*`, `/status` | Stock Jetstream v2 |

### Export

The response is newline-delimited JSON, one record per line, collections in
order and records ordered by DID and record key within each:

```json
{"uri":"at://did:plc:…/community.lexicon.calendar.event/3m…","cid":"bafy…","rev":"3m…","time_us":1794705518038027,"record":{…}}
```

`time_us` is when this version of the record was witnessed. The last line
ends the export:

```json
{"end":true,"cursor":51655620,"time_us":1794851843515262,"records":152700}
```

Then subscribe to Jetstream on the same host with that cursor (also sent up
front as `Alluvium-Cursor`, with `Alluvium-Time-Us` and `Alluvium-Snapshot`).
The live stream replays from the cursor inclusively.

To follow a different Jetstream instance, such as the official one, use
`time_us` as a timestamp cursor, minus a few minutes of margin: sequence
numbers are per instance, time is not. The snapshot must then be younger
than that instance's replay window (36 hours on the official ones);
`/alluvium/head` tells its time before you start.

A response without the end line was cut short. Repeat the request with
`after=<uri of the last record received>` and keep the first response's
cursor. Resuming works across snapshots: records keep their order, and
replaying from the first cursor reconciles whatever changed in between.

Between snapshots, an export still contains records that were deleted in the
meantime, including those of deleted accounts. The replay removes them.

Moderation labels are not applied. Accounts that are deactivated, suspended
or taken down at their PDS are left out.

`GET /collections` and `/alluvium/head` report `excludes`, the collections
this host does not carry, so an absent collection is not mistaken for an
empty one.

A `collection` that is not an NSID is answered with 400. One that is an
NSID but holds no records gives an export of no records.

If `ALLUVIUM_TOKENS_FILE` is set, `/export` and `/alluvium/status` need
`Authorization: Bearer <token>`.

### Compression

Send `Accept-Encoding: zstd` if your client can decode a zstd stream of many
frames: the snapshot stores exactly that, and the response is the stored
bytes. `zstd -d` and Go's `klauspost/compress` can; check yours.
**Node cannot**: `fetch` and `node:zlib` (as of 24.15) stop after the first
frame without an error, which looks like an export cut short. Use gzip
there, which is also what `fetch` asks for by default. For gzip, or no
compression, the server decodes and re-encodes on the fly.

## Snapshots

```
/var/lib/alluvium/snapshots/
  current -> 51655620          the snapshot being served
  51655620/                    named by its cursor
    manifest.json              cursor, time, excludes, and per collection its file and counts
    data/<file>.zst            one collection's export lines, as zstd frames
    data/<file>.idx            where each frame starts and with which record
    hidden/<file>.zst, .idx    records of hidden accounts, same format, not served
    accounts.zst               hidden accounts, and each account's collections
  build.json                   report of the last build
```

`zstd -dc data/<file>.zst` prints a collection; `manifest.json` maps NSIDs
to file names, which are hashes because NSIDs differ by case and can exceed
a file name. The previous snapshot is kept until the next build for requests
still reading it.

**A snapshot is a pure function of the events it covers.** A build from
scratch and any chain of incremental builds write byte-identical files
(`manifest.json` differs in `built_at`). A change of format is therefore
handled by building from scratch, never by migration; `alluvium build` does
that by itself when it finds a snapshot of another format version.

How events fold into records:

| Event | Effect |
|---|---|
| commit create or update | the record is this version |
| commit delete | the record is removed |
| `#sync` | every record of the account is removed; the resync's creates follow |
| `#account` deleted | every record of the account is removed; it is no longer hidden |
| `#account` active | the account is no longer hidden |
| `#account` deactivated, suspended, taken down, other | the account is hidden: records kept in `hidden/`, not served |

Hidden records are carried from snapshot to snapshot so that a reactivation
needs no refetch. An account's hidden state is remembered in `accounts.zst`
even while it has no records, so records that arrive later are hidden too.

Records are ordered by collection, DID and record key, each compared on its
own. Never order by AT URI: `did:web:a.example` sorts after `did:web:a` as a
DID but before it inside a URI, where `/` follows.

### Frames

Each collection file is a run of independent zstd frames of whole lines,
about 1 MiB of JSON each (256 KiB to 4 MiB). Frame boundaries are
content-defined: a frame ends after a record picked by a hash of its key.
Boundaries therefore depend on the records around them and not on how the
file came to be, so an incremental build re-encodes only the frames near a
change, copies every other frame's bytes, and still writes what a build from
scratch would. A collection without changes is hard-linked whole.

Measured on real collections, with klauspost/compress at its default level:

| Frame size | `site.standard.document` | `fm.teal.feed.play` | `so.sprk.feed.post` |
|---|---|---|---|
| 256 KiB | 425 B/record | 91.7 | 292.6 |
| 1 MiB | 409 | 85.3 | 284.6 |
| 4 MiB | 403 | 81.6 | 282.9 |
| 16 MiB | 401 | 80.3 | 282.2 |

Frames four times larger would save 1 to 4 percent and make every change
re-encode four times as much. The next compression level saves about 3
percent for 40 percent more CPU. No dictionaries: a client could not decode
the stored frames without them.

Frames are always written by the same pure-Go encoder, one frame per call,
so identical lines give identical bytes on every platform.

## Develop

Go 1.27.

```sh
go test ./...
go build -o bin/alluvium ./cmd/alluvium
(cd ../jetstream && go build -o ../alluvium/bin/jetstream ./cmd/jetstream)
```

Run it locally against the real network with a small repo list:

```sh
printf 'community.lexicon.calendar.event\n' > nsids.txt
bin/alluvium discover -status-urls "" -nsids-file nsids.txt -out repos.txt -contact <your handle>

JETSTREAM_RELAY_URL=https://bsky.network JETSTREAM_PLC_URL=https://plc.directory \
JETSTREAM_DATA_DIR=./data/jetstream JETSTREAM_INGEST_DENY_COLLECTIONS='app.bsky.*,chat.bsky.*' \
JETSTREAM_BACKFILL_REPO_LIST=./repos.txt GOMEMLIMIT=2GiB \
bin/jetstream serve --addr 127.0.0.1:8080 --failed-repo-retry-interval=1m

bin/alluvium build -snapshots ./data/snapshots    # again whenever you want a newer one
bin/alluvium serve -snapshots ./data/snapshots
```

`alluvium build -through <seq>` stops at a sequence number instead of at
the present, which makes builds repeatable for tests.

The general appview that used to live here (record queries, backlinks,
profiles, labels, on a Pebble store folded live from Jetstream) is kept as
the tag `appview-v3`.

## Deploy

`deploy/` holds systemd units, environment files, a Caddyfile and
`install.sh` for a Debian or Ubuntu host. Cross-compile both binaries with
`CGO_ENABLED=0 GOOS=linux GOARCH=amd64`, copy them and `deploy/` to the host,
and run `deploy/install.sh ./jetstream ./alluvium` as root.

| Path | |
|---|---|
| `/etc/alluvium/*.env` | Settings; never overwritten by the installer |
| `/var/lib/alluvium/jetstream` | Jetstream archive and metadata |
| `/var/lib/alluvium/snapshots` | Snapshots; safe to delete, the next build starts from scratch |
| `/var/lib/alluvium/repos.txt` | Repo list written by discovery |

`alluvium-build.timer` starts a build every hour; one that is still running
is not started again. To rebuild from scratch, delete
`/var/lib/alluvium/snapshots/*` and `systemctl start --no-block alluvium-build`.
Exports answer 503 until it publishes.

Watch `/alluvium/health`. It measures the age of the snapshot's last event,
so it fails when builds fail and also when Jetstream stops receiving events.

The ingest filter is permanent for what it drops: widening it later means a
new backfill.

### Sizing

On the production box (Hetzner CX23: 2 shared vCPUs, 4 GB, 2026-11-17,
46.5M events, 45.6M records in 33,100 collections):

| | |
|---|---|
| Build from scratch | 48 min (38 reading the local archive), 65 CPU-minutes |
| Its memory | 1.3 GB peak, and 630 MB of swap; Jetstream at `GOMEMLIMIT=1650MiB` meanwhile |
| Its disk | 11 GB before, 24 GB at peak |
| Catch-up build after 15 hours (1.56M events) | 7 min, 1.3 GB peak |
| Disk with two snapshots | 26 GB of 38 |

The rest is from an M-series Mac building from the same archive the day
before:

| | |
|---|---|
| Snapshot | 7.8 GB (the Pebble store it replaces was 12.5 GB) |
| Build from scratch | 39 min, of which 34 downloading the archive; 12 CPU-minutes |
| Its scratch space | 8.5 GB of sorted runs, next to the snapshot: 17 GB at peak |
| Its memory | 1.5 GB resident with `GOMEMLIMIT=1200MiB` |
| Hourly build (66,000 events) | 2 min, 34 CPU-seconds, 0.5 GB resident |
| What it rewrites | 5.2 GB in 575 collections; 2.6 GB in 32,400 hard-linked |
| Frames it re-encodes | 28% of the changed collections' frames; the rest are copied |
| Export, zstd | 1 GB/s of stored bytes (1.7M `fm.teal.feed.play` records in 0.15 s) |
| Export, gzip | 300 MB/s of JSON, one core |

- **Disk**: a build needs the current snapshot and the new one side by
  side, since the large collections change every hour: about twice the
  snapshot, plus the Jetstream archive. The snapshot before the current one
  is removed when a build starts.
- **Memory**: a build from scratch is the expensive one. The Jetstream
  client holds the archive segment it is decoding and the next one (about
  270 MB each), and the sorter 128 MiB. Hourly builds read the live tail and
  hold neither. On a 4 GB host, lower Jetstream's `GOMEMLIMIT` to about
  1.65 GiB for the first build, as for the old first fold.
- **CPU**: `alluvium-build.service` runs at low CPU and I/O priority so
  Jetstream keeps up with the firehose; expect builds on 2 shared vCPUs to
  take several times the figures above.
- The bootstrap of 300,000 repos took about 22 hours.

## Known limits

- The archive follows the relay. When the relay falls behind on a PDS, so
  does every export: on 2026-11-18 it was 40 hours behind on one busy PDS,
  and 3,000 of its calendar events were missing here.
- Records that break the data model are not archived, most often because
  they hold a float (`"lat": 38.25`). They can still be read from their PDS.
- A repo is archived without the records whose blocks its PDS has lost,
  once the PDS confirms it cannot serve them.
- Coverage depends on the collection directories. A repo with old records
  that neither Lightrail nor the relay lists is missed until it writes again.
- A record whose JSON form the Jetstream client cannot decode from the live
  stream (for example a malformed CID link) is skipped and logged. The same
  record read from the archive decodes, so a build from scratch includes it.
- A build reads through the Jetstream client, which holds a whole archive
  segment (about 270 MB) in memory while replaying sealed history.
- There are no per-caller rate limits or concurrency caps yet.
