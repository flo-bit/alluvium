# Deploy

`deploy/` holds systemd units, environment files, a Caddyfile and
`install.sh` for a Debian or Ubuntu host. Cross-compile both binaries with
`CGO_ENABLED=0 GOOS=linux GOARCH=amd64`, copy them and `deploy/` to the host,
and run `deploy/install.sh ./jetstream ./alluvium` as root.

Before you start the services, set your own values:

- `deploy/Caddyfile`: the host name
- `/etc/alluvium/alluvium.env`: `ALLUVIUM_CONTACT`
- `/etc/alluvium/jetstream.env`: `JETSTREAM_USER_AGENT`

The contact is sent to the relay, the collection directories and every PDS
a repo is downloaded from. The memory limits in the units and environment
files are sized for a 4 GB host.

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

## Sizing

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
| Snapshot | 7.8 GB |
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
  1.65 GiB for the first build.
- **CPU**: `alluvium-build.service` runs at low CPU and I/O priority so
  Jetstream keeps up with the firehose; expect builds on 2 shared vCPUs to
  take several times the figures above.
- The bootstrap of 300,000 repos took about 22 hours.
