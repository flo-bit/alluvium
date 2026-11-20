# Alluvium

Fast backfill for Atmosphere records outside the Bluesky app.

Ask for a collection and get every current record of it in one streamed
response, then follow the live stream from the cursor it ends with.

```sh
curl --compressed 'https://backfill.atmo.tools/export?collection=community.lexicon.calendar.event'
```

This is a prototype: the API may change, and the public instance is one
small box.

## How it works

- `alluvium discover` lists every repo that holds records outside
  `app.bsky.*` and `chat.bsky.*`.
- A [Jetstream fork](https://github.com/flo-bit/jetstream/tree/alluvium)
  backfills those repos and archives the firehose without the Bluesky
  collections.
- `alluvium build` folds its events into a snapshot of all current records,
  every hour.
- `alluvium serve` streams exports straight from the snapshot files.

## Docs

- [API](docs/api.md)
- [Snapshots](docs/snapshots.md)
- [Develop](docs/develop.md)
- [Deploy](docs/deploy.md)
- [Known limits](docs/limits.md)

MIT licensed.
