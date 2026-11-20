# Develop

Go 1.27.

```sh
go test ./...
go build -o bin/alluvium ./cmd/alluvium

git clone -b alluvium https://github.com/flo-bit/jetstream
(cd jetstream && go build -o ../bin/jetstream ./cmd/jetstream)
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
