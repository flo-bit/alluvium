# API

`https://backfill.atmo.tools`

| Route | |
|---|---|
| `GET /export?collection=A&collection=B[&after=<uri>]` | Current records of up to 100 collections |
| `GET /collections` | The snapshot's head and every collection with its record count |
| `GET /alluvium/head` | The snapshot being served: cursor, its time, excluded collections |
| `GET /alluvium/health` | 200 while the snapshot's last event is under three hours old |
| `GET /alluvium/status` | Snapshot totals, the last build's report, export usage |
| `/subscribe`, `/xrpc/network.bsky.jetstream.*`, `/status` | Stock Jetstream v2 |

## Export

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

## Compression

Send `Accept-Encoding: zstd` if your client can decode a zstd stream of many
frames: the snapshot stores exactly that, and the response is the stored
bytes. `zstd -d` and Go's `klauspost/compress` can; check yours.
**Node cannot**: `fetch` and `node:zlib` (as of 24.15) stop after the first
frame without an error, which looks like an export cut short. Use gzip
there, which is also what `fetch` asks for by default. For gzip, or no
compression, the server decodes and re-encodes on the fly.
