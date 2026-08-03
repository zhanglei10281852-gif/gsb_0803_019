# Live Segment Publish Coordinator

A headless service that coordinates live-stream segment publishing. Transcode
producers report segments in batches per `(stream, rendition, generation)`;
the service tracks per-rendition contiguous progress and a stream-wide publish
watermark, with idempotent, all-or-nothing submissions.

Built with Go 1.24+ and the standard library only — no database, message
queue, external cache, or web framework.

## Architecture

Responsibilities are split into layers so transport, coordination, and storage
stay independent:

- `internal/domain` — the `Stream` aggregate. Pure coordination logic:
  validation, idempotency, conflict detection, contiguous `head` computation,
  `watermark` derivation, and the monotonic `revision`. Every method assumes
  single-threaded (exclusive) access.
- `internal/storage` — in-memory registry. Owns concurrency: a per-stream
  mutex serializes all operations on a stream, so concurrent requests are
  equivalent to *some* serial order and the observed revision always matches
  committed state.
- `internal/service` — orchestration between transport and domain over the
  store. Snapshots are taken under the same lock as the mutation.
- `internal/httpapi` — the JSON/HTTP wire contract and error→status mapping.
- `cmd/coordinator` — process wiring and graceful shutdown.

## Core semantics

- **Revision** is a per-stream monotonic counter. It advances by exactly one
  for every applied submission. An idempotent replay does **not** advance it.
- **Head** (per rendition, current generation) is the largest sequence `H`
  such that sequences `0..H` are all present. It is `null` when sequence `0`
  is missing and never jumps a hole — out-of-order segments are buffered until
  the gap fills.
- **Watermark** (per stream) is the minimum head across **all active
  renditions** of the **current generation** (the highest generation observed).
  It is `null` unless every active rendition has a head in that generation.
- **Generations**: status and watermark are always scoped to the current
  generation. Starting a new generation resets the watermark until the new
  generation is contiguous across all renditions.

### Idempotency, conflict, atomicity

- A submission is keyed by `(rendition, generation, submissionId)`.
- Re-submitting the **identical content** (segment set, order-independent) is
  idempotent: no state change, no revision bump.
- Re-using a `submissionId` with **different content** is a `409 conflict`.
- Submitting a sequence already committed with **different bytes** (even under
  a new `submissionId`) is a `409 conflict`.
- A batch is all-or-nothing: all validation and conflict checks run before any
  write, so a rejected batch leaves no partial segments.

## HTTP contract

All bodies are JSON. Numbers for `sequence`, `durationMs`, `generation`,
`revision`, `head`, and `watermark` are non-negative integers. Absent progress
is represented as JSON `null`.

### `GET /healthz`
`200` → `{"status":"ok"}`

### `POST /v1/streams`
Declare a stream and its active renditions. Idempotent on identical
declarations.

Request:
```json
{ "streamId": "game-42", "renditions": ["720p", "1080p"] }
```
- `201 Created` — new stream (returns status view).
- `200 OK` — identical re-declaration (returns status view).
- `409 Conflict` — stream exists with a different rendition set.
- `400 Bad Request` — missing id / empty or duplicate renditions.

### `POST /v1/streams/{streamId}/segments`
Submit a batch for one rendition + generation.

Request:
```json
{
  "rendition": "720p",
  "generation": 1,
  "submissionId": "producer-a-000123",
  "segments": [
    { "sequence": 0, "durationMs": 2000, "sha256": "<64 hex chars>" },
    { "sequence": 1, "durationMs": 2000, "sha256": "<64 hex chars>" }
  ]
}
```
- `201 Created` — batch applied (`"applied": true`).
- `200 OK` — idempotent replay (`"applied": false`, revision unchanged).
- `409 Conflict` — submissionId reused with different content, or a sequence
  rewritten with different bytes.
- `400 Bad Request` — empty batch, non-positive duration, invalid sha256,
  unknown rendition, empty submissionId, duplicate sequence within the batch.
- `404 Not Found` — stream does not exist.

Response body (`submitView`):
```json
{ "applied": true, "status": { /* status view */ } }
```

### `GET /v1/streams/{streamId}`
Return the current status snapshot.

- `200 OK`:
```json
{
  "streamId": "game-42",
  "revision": 3,
  "activeRenditions": ["1080p", "720p"],
  "currentGeneration": 1,
  "watermark": 0,
  "renditions": {
    "720p": { "generation": 1, "head": 1, "segmentCount": 2, "buffered": [] },
    "1080p": { "generation": 1, "head": 0, "segmentCount": 1, "buffered": [] }
  }
}
```
- `404 Not Found` — unknown stream.

Error responses use `{ "error": "<message>", "code": "<label>" }` where `code`
is one of `validation`, `conflict`, `already_exists`, `not_found`, `internal`.

## Running

```bash
go run ./cmd/coordinator            # listens on :8080
COORDINATOR_ADDR=:9000 go run ./cmd/coordinator
```

## Tests

```bash
go test ./...
```

Coverage includes domain unit tests (head/watermark/generation math,
idempotency, conflict, batch atomicity), service concurrency tests
(serializable revisions, concurrent replay), and real HTTP regression tests
via `httptest` (full request/response cycle, status codes, concurrency).
