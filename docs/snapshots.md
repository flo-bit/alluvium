# Snapshots

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

## Frames

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
