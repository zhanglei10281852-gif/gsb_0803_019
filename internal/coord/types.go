// Package coord implements the domain coordination and in-memory storage for
// the live stream segment publish coordinator.
package coord

import "fmt"

// Segment is a single media segment reported by a transcoding producer.
type Segment struct {
	Sequence   int64  `json:"sequence"`
	DurationMs int64  `json:"durationMs"`
	SHA256     string `json:"sha256"`
}

// IngestRequest is one batch of segments, identified by a submission ID.
type IngestRequest struct {
	SubmissionID string    `json:"submissionId"`
	Rendition    string    `json:"rendition"`
	Generation   int64     `json:"generation"`
	Segments     []Segment `json:"segments"`
}

// IngestResult is the committed outcome of an ingest batch. Revision is the
// stream revision at which this batch was committed; on an idempotent replay
// the originally committed result is returned with Replayed set to true.
type IngestResult struct {
	StreamID   string `json:"streamId"`
	Rendition  string `json:"rendition"`
	Generation int64  `json:"generation"`
	Revision   int64  `json:"revision"`
	Head       int64  `json:"head"`
	Watermark  int64  `json:"watermark"`
	Applied    int    `json:"applied"`
	Replayed   bool   `json:"replayed"`
}

// RenditionStatus exposes the contiguous head of one active rendition in the
// stream's current generation. Head is the last sequence of the gap-free
// prefix starting at sequence 0, or -1 when empty. Buffered counts segments
// stored beyond the head (out-of-order, waiting on gaps).
type RenditionStatus struct {
	Head     int64 `json:"head"`
	Buffered int   `json:"buffered"`
}

// Status is the published state of a stream.
type Status struct {
	StreamID   string                     `json:"streamId"`
	Revision   int64                      `json:"revision"`
	Generation int64                      `json:"generation"`
	Watermark  int64                      `json:"watermark"`
	Renditions map[string]RenditionStatus `json:"renditions"`
}

// Stable machine-readable error codes returned by the Coordinator and
// surfaced in the HTTP API.
const (
	CodeInvalidArgument    = "invalid_argument"
	CodeNotFound           = "not_found"
	CodeAlreadyExists      = "already_exists"
	CodeStaleGeneration    = "stale_generation"
	CodeSubmissionConflict = "submission_conflict"
	CodeSegmentConflict    = "segment_conflict"
)

// Error is a domain error with a stable machine-readable code.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func errorf(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}
