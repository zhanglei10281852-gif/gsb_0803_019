package domain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"sort"
	"sync"
)

var sha256Hex = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

type Coordinator struct {
	store   StreamStore
	locksMu sync.Mutex
	locks   map[string]*sync.RWMutex
}

type StreamStore interface {
	CreateStream(ctx context.Context, s *Stream) error
	GetStream(ctx context.Context, streamID string) (*Stream, bool)
}

func NewCoordinator(store StreamStore) *Coordinator {
	return &Coordinator{
		store: store,
		locks: make(map[string]*sync.RWMutex),
	}
}

func (c *Coordinator) streamLock(id string) *sync.RWMutex {
	c.locksMu.Lock()
	defer c.locksMu.Unlock()
	l, ok := c.locks[id]
	if !ok {
		l = &sync.RWMutex{}
		c.locks[id] = l
	}
	return l
}

func (c *Coordinator) Health(ctx context.Context) HealthView {
	return HealthView{Status: "ok"}
}

func (c *Coordinator) CreateStream(ctx context.Context, in CreateStreamInput) (*StreamView, error) {
	if in.StreamID == "" {
		return nil, NewError(ErrCodeInvalidRequest, "streamId is required")
	}
	if len(in.ActiveRenditions) == 0 {
		return nil, NewError(ErrCodeInvalidRequest, "activeRenditions must not be empty")
	}
	active := make(map[string]struct{}, len(in.ActiveRenditions))
	for _, name := range in.ActiveRenditions {
		if name == "" {
			return nil, NewError(ErrCodeInvalidRequest, "rendition name must not be empty")
		}
		if _, dup := active[name]; dup {
			return nil, NewError(ErrCodeInvalidRequest, "duplicate rendition: "+name)
		}
		active[name] = struct{}{}
	}

	s := &Stream{
		ID:               in.StreamID,
		ActiveRenditions: active,
		Renditions:       make(map[string]*RenditionState, len(active)),
		Submissions:      make(map[string]*SubmissionRecord),
	}
	for name := range active {
		s.Renditions[name] = newRenditionState(name)
	}

	if err := c.store.CreateStream(ctx, s); err != nil {
		return nil, err
	}
	return streamToView(s), nil
}

func (c *Coordinator) SubmitSegments(ctx context.Context, in SubmitSegmentsInput) (*SubmitResultView, error) {
	if err := validateSubmitInput(in); err != nil {
		return nil, err
	}

	s, ok := c.store.GetStream(ctx, in.StreamID)
	if !ok {
		return nil, NewError(ErrCodeStreamNotFound, "stream not found: "+in.StreamID)
	}

	l := c.streamLock(s.ID)
	l.Lock()
	defer l.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if _, active := s.ActiveRenditions[in.Rendition]; !active {
		return nil, NewError(ErrCodeInvalidRequest, "rendition is not active for this stream: "+in.Rendition)
	}

	if s.GenerationSet && in.Generation < s.CurrentGeneration {
		return nil, NewError(ErrCodeStaleGeneration, fmt.Sprintf(
			"generation %d is stale; current generation is %d", in.Generation, s.CurrentGeneration))
	}

	if !s.GenerationSet || in.Generation > s.CurrentGeneration {
		s.CurrentGeneration = in.Generation
		s.GenerationSet = true
		resetRenditions(s)
		s.Submissions = make(map[string]*SubmissionRecord)
	}

	key := submissionKey(in.Rendition, in.Generation, in.SubmissionID)
	contentHash := submissionContentHash(in.Segments)

	if rec, exists := s.Submissions[key]; exists {
		if rec.ContentHash != contentHash {
			return nil, NewError(ErrCodeConflict, fmt.Sprintf(
				"submissionId %q already exists for this stream/rendition/generation with different content",
				in.SubmissionID))
		}
		rs := s.Renditions[in.Rendition]
		return &SubmitResultView{
			StreamID:     s.ID,
			Rendition:    in.Rendition,
			Generation:   s.CurrentGeneration,
			SubmissionID: in.SubmissionID,
			Revision:     rec.AppliedRevision,
			Idempotent:   true,
			Accepted:     0,
			HeadSequence: rs.HeadSequence,
			Watermark:    computeWatermark(s),
		}, nil
	}

	rs := s.Renditions[in.Rendition]

	newSegments := make([]Segment, 0, len(in.Segments))
	seen := make(map[int64]struct{}, len(in.Segments))
	for _, seg := range in.Segments {
		if seg.Sequence < 0 {
			return nil, NewError(ErrCodeInvalidRequest, "segment sequence must be >= 0")
		}
		if seg.DurationMs <= 0 {
			return nil, NewError(ErrCodeInvalidRequest, "segment durationMs must be > 0")
		}
		normalized := normalizeSha256(seg.Sha256)
		if !sha256Hex.MatchString(normalized) {
			return nil, NewError(ErrCodeInvalidRequest, fmt.Sprintf(
				"segment %d has invalid sha256: %q", seg.Sequence, seg.Sha256))
		}
		seg.Sha256 = normalized

		if _, dup := seen[seg.Sequence]; dup {
			return nil, NewError(ErrCodeInvalidRequest, fmt.Sprintf(
				"duplicate sequence %d in submission", seg.Sequence))
		}
		seen[seg.Sequence] = struct{}{}

		if existing, ok := rs.Segments[seg.Sequence]; ok {
			if existing.Sha256 != seg.Sha256 || existing.DurationMs != seg.DurationMs {
				return nil, NewError(ErrCodeConflict, fmt.Sprintf(
					"segment %d already exists with different content", seg.Sequence))
			}
			continue
		}
		newSegments = append(newSegments, seg)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for _, seg := range newSegments {
		rs.Segments[seg.Sequence] = seg
	}
	advanceHead(rs)

	s.Revision++
	appliedRevision := s.Revision
	s.Submissions[key] = &SubmissionRecord{
		SubmissionID:    in.SubmissionID,
		Rendition:       in.Rendition,
		Generation:      in.Generation,
		ContentHash:     contentHash,
		AppliedRevision: appliedRevision,
	}

	return &SubmitResultView{
		StreamID:     s.ID,
		Rendition:    in.Rendition,
		Generation:   s.CurrentGeneration,
		SubmissionID: in.SubmissionID,
		Revision:     appliedRevision,
		Idempotent:   false,
		Accepted:     len(newSegments),
		HeadSequence: rs.HeadSequence,
		Watermark:    computeWatermark(s),
	}, nil
}

func (c *Coordinator) GetStatus(ctx context.Context, streamID string) (*StreamView, error) {
	s, ok := c.store.GetStream(ctx, streamID)
	if !ok {
		return nil, NewError(ErrCodeStreamNotFound, "stream not found: "+streamID)
	}
	l := c.streamLock(s.ID)
	l.RLock()
	defer l.RUnlock()
	return streamToView(s), nil
}

func newRenditionState(name string) *RenditionState {
	return &RenditionState{
		Name:     name,
		Segments: make(map[int64]Segment),
	}
}

func resetRenditions(s *Stream) {
	for name := range s.ActiveRenditions {
		s.Renditions[name] = newRenditionState(name)
	}
	for name := range s.Renditions {
		if _, active := s.ActiveRenditions[name]; !active {
			delete(s.Renditions, name)
		}
	}
}

func advanceHead(rs *RenditionState) {
	for {
		seg, ok := rs.Segments[rs.HeadSequence]
		if !ok {
			return
		}
		rs.ContiguousMs += seg.DurationMs
		rs.HeadSequence++
	}
}

func computeWatermark(s *Stream) WatermarkView {
	wmSeq := int64(math.MaxInt64)
	wmMs := int64(math.MaxInt64)
	for name := range s.ActiveRenditions {
		rs := s.Renditions[name]
		if rs.HeadSequence < wmSeq {
			wmSeq = rs.HeadSequence
		}
		if rs.ContiguousMs < wmMs {
			wmMs = rs.ContiguousMs
		}
	}
	if wmSeq == math.MaxInt64 {
		return WatermarkView{}
	}
	return WatermarkView{Sequence: wmSeq, TimeMs: wmMs}
}

func streamToView(s *Stream) *StreamView {
	active := make([]string, 0, len(s.ActiveRenditions))
	for name := range s.ActiveRenditions {
		active = append(active, name)
	}
	sort.Strings(active)

	renditions := make(map[string]RenditionView, len(s.Renditions))
	for name, rs := range s.Renditions {
		buffered := len(rs.Segments) - int(rs.HeadSequence)
		if buffered < 0 {
			buffered = 0
		}
		renditions[name] = RenditionView{
			HeadSequence:     rs.HeadSequence,
			ContiguousTimeMs: rs.ContiguousMs,
			BufferedSegments: buffered,
		}
	}

	gen := int64(0)
	if s.GenerationSet {
		gen = s.CurrentGeneration
	}

	return &StreamView{
		StreamID:         s.ID,
		Revision:         s.Revision,
		Generation:       gen,
		ActiveRenditions: active,
		Renditions:       renditions,
		Watermark:        computeWatermark(s),
	}
}

func validateSubmitInput(in SubmitSegmentsInput) error {
	if in.StreamID == "" {
		return NewError(ErrCodeInvalidRequest, "streamId is required")
	}
	if in.Rendition == "" {
		return NewError(ErrCodeInvalidRequest, "rendition is required")
	}
	if in.SubmissionID == "" {
		return NewError(ErrCodeInvalidRequest, "submissionId is required")
	}
	if in.Generation < 0 {
		return NewError(ErrCodeInvalidRequest, "generation must be >= 0")
	}
	if len(in.Segments) == 0 {
		return NewError(ErrCodeInvalidRequest, "segments must not be empty")
	}
	return nil
}

func submissionKey(rendition string, generation int64, submissionID string) string {
	return fmt.Sprintf("%s|%d|%s", rendition, generation, submissionID)
}

func submissionContentHash(segments []Segment) string {
	sorted := make([]Segment, len(segments))
	copy(sorted, segments)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Sequence < sorted[j].Sequence
	})
	h := sha256.New()
	for _, seg := range sorted {
		fmt.Fprintf(h, "%d|%d|%s;", seg.Sequence, seg.DurationMs, seg.Sha256)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func normalizeSha256(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'F' {
			b[i] = b[i] + ('a' - 'A')
		}
	}
	return string(b)
}
