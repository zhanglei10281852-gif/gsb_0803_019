package domain

import "fmt"

const (
	ErrCodeStreamNotFound  = "STREAM_NOT_FOUND"
	ErrCodeStreamExists    = "STREAM_ALREADY_EXISTS"
	ErrCodeInvalidRequest  = "INVALID_REQUEST"
	ErrCodeConflict        = "CONFLICT"
	ErrCodeStaleGeneration = "STALE_GENERATION"
	ErrCodeInternal        = "INTERNAL"
)

type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

type Segment struct {
	Sequence   int64  `json:"sequence"`
	DurationMs int64  `json:"durationMs"`
	Sha256     string `json:"sha256"`
}

type RenditionState struct {
	Name         string
	HeadSequence int64
	ContiguousMs int64
	Segments     map[int64]Segment
}

type SubmissionRecord struct {
	SubmissionID    string
	Rendition       string
	Generation      int64
	ContentHash     string
	AppliedRevision int64
}

type Stream struct {
	ID                string
	ActiveRenditions  map[string]struct{}
	CurrentGeneration int64
	GenerationSet     bool
	Renditions        map[string]*RenditionState
	Revision          int64
	Submissions       map[string]*SubmissionRecord
}

type CreateStreamInput struct {
	StreamID         string
	ActiveRenditions []string
}

type SubmitSegmentsInput struct {
	StreamID     string
	Rendition    string
	Generation   int64
	SubmissionID string
	Segments     []Segment
}

type WatermarkView struct {
	Sequence int64 `json:"sequence"`
	TimeMs   int64 `json:"timeMs"`
}

type RenditionView struct {
	HeadSequence     int64 `json:"headSequence"`
	ContiguousTimeMs int64 `json:"contiguousTimeMs"`
	BufferedSegments int   `json:"bufferedSegments"`
}

type StreamView struct {
	StreamID         string                   `json:"streamId"`
	Revision         int64                    `json:"revision"`
	Generation       int64                    `json:"generation"`
	ActiveRenditions []string                 `json:"activeRenditions"`
	Renditions       map[string]RenditionView `json:"renditions"`
	Watermark        WatermarkView            `json:"watermark"`
}

type SubmitResultView struct {
	StreamID     string        `json:"streamId"`
	Rendition    string        `json:"rendition"`
	Generation   int64         `json:"generation"`
	SubmissionID string        `json:"submissionId"`
	Revision     int64         `json:"revision"`
	Idempotent   bool          `json:"idempotent"`
	Accepted     int           `json:"accepted"`
	HeadSequence int64         `json:"headSequence"`
	Watermark    WatermarkView `json:"watermark"`
}

type HealthView struct {
	Status string `json:"status"`
}
