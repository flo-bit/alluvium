# Known limits

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
