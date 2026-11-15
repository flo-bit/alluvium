# Alluvium

Fast backfill for Atmosphere records outside the Bluesky app.

Ask for a collection and get every current record of it in one streamed
response, then follow the live stream from the cursor it ends with.

```sh
curl --compressed 'https://backfill.atmo.tools/export?collection=community.lexicon.calendar.event'
```

## How it works

Two processes on one machine:

- **Jetstream** (`../jetstream`, branch `alluvium`) is a fork of
  [Jetstream v2](https://github.com/bluesky-social/jetstream) with three
  small additions: an ingest filter that drops `app.bsky.*` and `chat.bsky.*`,
  backfill from a repo list instead of the whole network, and a configurable
  User-Agent. It verifies and archives the firehose, backfills listed repos
  from their PDS, and serves the stock Jetstream v2 API.
- **`alluvium serve`** is a Jetstream client on localhost. It folds events
  into a current-state store keyed by collection (deletes applied, deleted
  accounts purged, inactive accounts hidden) and serves exports from it.

`alluvium discover` builds the repo list: it takes every collection name the
official Jetstream instance has seen, asks Lightrail and the relay which repos
hold them, and resolves each repo's PDS. Jetstream re-reads the list on every
retry pass, so a daily run keeps coverage current.

## API

`https://backfill.atmo.tools`

| Route | |
|---|---|
| `GET /export?collection=A&collection=B` | Current records of up to 100 collections |
| `GET /collections` | Every collection with its record count |
| `GET /alluvium/health` | 200 while within five minutes of the firehose |
| `GET /alluvium/status` | Cursor, lag, totals, export usage |
| `/subscribe`, `/xrpc/network.bsky.jetstream.*`, `/status` | Stock Jetstream v2 |

### Query

Three methods under `/xrpc/tools.atmo.index.`, with no per-app configuration:

| Method | Parameters |
|---|---|
| `getRecord` | `uri` |
| `listRecords` | `collection`, `actor` (DID or handle), `order` (`desc` or `asc`), `limit` (1-200), `cursor` |
| `getProfile` | `actor` |

Records are ordered by the creation time in their record key, falling back
to when they were first seen. `getRecord` and `listRecords` also take:

| Parameter | Effect |
|---|---|
| `<field>=<value>` | Keep records whose field equals the value (for an array, contains it). Dotted paths reach nested fields. |
| `<field>Min`, `<field>Max` | Range bounds; numbers, datetimes and strings compare as such. |
| `backlinks=<collection>:<path>` | Per record, how many records of that collection link to it at that path, and from how many accounts. Up to 10. |
| `hydrate=<path>` | Attach the record the AT URI at that path points to. Up to 5. |
| `profiles=true` or `profiles=<collection>,…` | Add each author's handle and profile records. `true` means the Bluesky profile, fetched from the PDS and cached for an hour. |

A filter on an AT URI or DID uses the backlink index. Other filters examine
candidates in time order, at most 50,000 per request; a response can then
hold fewer records than `limit`, or none, and still carry a cursor to
continue from.

Send `Atproto-Accept-Labelers: <did>, <did>;redact` to have those labelers'
labels attached to records and profiles (labels on the record and on its
author). `;redact` also drops records that labeler has taken down. A labeler
named for the first time starts syncing from its first label and is left out
of responses until it has caught up, which takes hours for a large one;
`Atproto-Content-Labelers` lists the labelers actually applied.

```sh
curl 'https://backfill.atmo.tools/xrpc/tools.atmo.index.listRecords?collection=social.grain.gallery&limit=24&backlinks=social.grain.favorite:subject&profiles=true'
```

### Export

The response is newline-delimited JSON, compressed with zstd or gzip per
`Accept-Encoding`. Each line is a Jetstream commit event with
`"operation":"create"`, so one handler serves both the export and the live
stream:

```json
{"did":"did:plc:…","cursor":218626,"time_us":1794705518038027,"kind":"commit","commit":{"operation":"create","collection":"…","rkey":"…","rev":"…","cid":"…","record":{…}}}
```

The last line ends the export:

```json
{"kind":"end","cursor":3688002,"records":139398}
```

Then subscribe to Jetstream on the same host with that cursor (also sent up
front in the `Alluvium-Cursor` header). The live stream replays from the
cursor inclusively; applying its events on top of the export is idempotent.

A response without the end line was cut short. To resume, request with
`resume=1` so each line carries a `resume` token, then repeat the request
with `after=<last token>`. Keep the cursor of the first response.

Per-line `cursor` values are not ordered; use the end line's.

If `ALLUVIUM_TOKENS_FILE` is set, `/export` and `/alluvium/status` need
`Authorization: Bearer <token>`.

## Develop

Go 1.27.

```sh
go test ./...
go build -o bin/alluvium ./cmd/alluvium
(cd ../jetstream && go build -o ../alluvium/bin/jetstream ./cmd/jetstream)
```

Run the pair locally against the real network with a small repo list:

```sh
printf 'community.lexicon.calendar.event\n' > nsids.txt
bin/alluvium discover -status-urls "" -nsids-file nsids.txt -out repos.txt -contact <your handle>

JETSTREAM_RELAY_URL=https://bsky.network JETSTREAM_PLC_URL=https://plc.directory \
JETSTREAM_DATA_DIR=./data/jetstream JETSTREAM_INGEST_DENY_COLLECTIONS='app.bsky.*,chat.bsky.*' \
JETSTREAM_BACKFILL_REPO_LIST=./repos.txt GOMEMLIMIT=2GiB \
bin/jetstream serve --addr 127.0.0.1:8080 --failed-repo-retry-interval=1m

bin/alluvium serve -data-dir ./data/store
```

## Deploy

`deploy/` holds systemd units, environment files, a Caddyfile and
`install.sh` for a Debian or Ubuntu host. Cross-compile both binaries with
`CGO_ENABLED=0 GOOS=linux GOARCH=amd64`, copy them and `deploy/` to the host,
and run `deploy/install.sh ./jetstream ./alluvium` as root.

| Path | |
|---|---|
| `/etc/alluvium/*.env` | Settings; never overwritten by the installer |
| `/var/lib/alluvium/jetstream` | Jetstream archive and metadata |
| `/var/lib/alluvium/store` | Export store; safe to delete, it refolds from Jetstream |
| `/var/lib/alluvium/repos.txt` | Repo list written by discovery |

The ingest filter is permanent for what it drops: widening it later means a
new backfill.

## Known limits

- Coverage depends on the collection directories. A repo with old records
  that neither Lightrail nor the relay lists is missed until it writes again.
- A record whose JSON form the Jetstream client cannot decode (for example a
  malformed CID link) is skipped by the export store and logged.
- Records of deactivated, suspended or taken-down accounts are hidden, not
  removed, until the account is deleted.
- Backlink counts include links from accounts a caller's labelers flag.
- Backlinks from Bluesky records are not indexed.
- There are no per-caller rate limits yet.
