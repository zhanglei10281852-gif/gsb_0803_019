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
		Cutovers:         make(map[string]*CutoverRecord),
	}
	for name := range active {
		s.Renditions[name] = newRenditionState(name, 0)
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
			"generation %d is fenced; current generation is %d", in.Generation, s.CurrentGeneration))
	}

	if s.GenerationSet && in.Generation > s.CurrentGeneration {
		return nil, NewError(ErrCodeCutoverRequired, fmt.Sprintf(
			"generation %d is ahead of current %d; an atomic cutover is required to advance generation",
			in.Generation, s.CurrentGeneration))
	}

	if !s.GenerationSet {
		s.CurrentGeneration = in.Generation
		s.GenerationSet = true
	}

	key := submissionKey(in.Rendition, in.Generation, in.SubmissionID)

	rs := s.Renditions[in.Rendition]

	normalized := make([]Segment, len(in.Segments))
	seen := make(map[int64]struct{}, len(in.Segments))
	for i, seg := range in.Segments {
		if seg.Sequence < 0 {
			return nil, NewError(ErrCodeInvalidRequest, "segment sequence must be >= 0")
		}
		if seg.Sequence < rs.StartSequence {
			return nil, NewError(ErrCodeSegmentOutOfRange, fmt.Sprintf(
				"segment sequence %d is before rendition start sequence %d in generation %d",
				seg.Sequence, rs.StartSequence, in.Generation))
		}
		if seg.DurationMs <= 0 {
			return nil, NewError(ErrCodeInvalidRequest, "segment durationMs must be > 0")
		}
		seg.Sha256 = normalizeSha256(seg.Sha256)
		if !sha256Hex.MatchString(seg.Sha256) {
			return nil, NewError(ErrCodeInvalidRequest, fmt.Sprintf(
				"segment %d has invalid sha256: %q", seg.Sequence, in.Segments[i].Sha256))
		}
		if _, dup := seen[seg.Sequence]; dup {
			return nil, NewError(ErrCodeInvalidRequest, fmt.Sprintf(
				"duplicate sequence %d in submission", seg.Sequence))
		}
		seen[seg.Sequence] = struct{}{}
		normalized[i] = seg
	}

	contentHash := submissionContentHash(normalized)

	if rec, exists := s.Submissions[key]; exists {
		if rec.ContentHash != contentHash {
			return nil, NewError(ErrCodeConflict, fmt.Sprintf(
				"submissionId %q already exists for this stream/rendition/generation with different content",
				in.SubmissionID))
		}
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

	newSegments := make([]Segment, 0, len(normalized))
	for _, seg := range normalized {
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

func (c *Coordinator) Cutover(ctx context.Context, in CutoverInput) (*CutoverView, error) {
	if err := validateCutoverInput(in); err != nil {
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

	contentHash := cutoverContentHash(in.Generation, in.ExpectedRevision, in.Renditions)

	if rec, exists := s.Cutovers[in.CutoverID]; exists {
		if rec.ContentHash != contentHash {
			return nil, NewError(ErrCodeCutoverConflict, fmt.Sprintf(
				"cutoverId %q already exists with different content", in.CutoverID))
		}
		return cutoverToView(s, in.CutoverID, rec.AppliedRevision, true), nil
	}

	if in.ExpectedRevision != s.Revision {
		return nil, NewError(ErrCodeRevisionMismatch, fmt.Sprintf(
			"expectedRevision %d does not match current revision %d",
			in.ExpectedRevision, s.Revision))
	}

	if s.GenerationSet && in.Generation <= s.CurrentGeneration {
		return nil, NewError(ErrCodeStaleGeneration, fmt.Sprintf(
			"cutover generation %d must be greater than current generation %d",
			in.Generation, s.CurrentGeneration))
	}

	active := make(map[string]struct{}, len(in.Renditions))
	renditions := make(map[string]*RenditionState, len(in.Renditions))
	for name, start := range in.Renditions {
		active[name] = struct{}{}
		renditions[name] = newRenditionState(name, start)
	}

	s.ActiveRenditions = active
	s.Renditions = renditions
	s.CurrentGeneration = in.Generation
	s.GenerationSet = true
	s.Submissions = make(map[string]*SubmissionRecord)
	s.Revision++
	appliedRevision := s.Revision

	s.Cutovers[in.CutoverID] = &CutoverRecord{
		CutoverID:        in.CutoverID,
		Generation:       in.Generation,
		ExpectedRevision: in.ExpectedRevision,
		ContentHash:      contentHash,
		AppliedRevision:  appliedRevision,
		Renditions:       copyStartMap(in.Renditions),
	}

	return cutoverToView(s, in.CutoverID, appliedRevision, false), nil
}

func newRenditionState(name string, startSequence int64) *RenditionState {
	return &RenditionState{
		Name:          name,
		StartSequence: startSequence,
		HeadSequence:  startSequence,
		Segments:      make(map[int64]Segment),
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
	if len(s.ActiveRenditions) == 0 {
		return WatermarkView{}
	}
	var maxStart int64 = math.MinInt64
	minHead := int64(math.MaxInt64)
	minMs := int64(math.MaxInt64)
	for name := range s.ActiveRenditions {
		rs := s.Renditions[name]
		if rs.StartSequence > maxStart {
			maxStart = rs.StartSequence
		}
		if rs.HeadSequence < minHead {
			minHead = rs.HeadSequence
		}
		if rs.ContiguousMs < minMs {
			minMs = rs.ContiguousMs
		}
	}
	if minHead >= maxStart {
		return WatermarkView{Sequence: minHead, TimeMs: minMs}
	}
	return WatermarkView{Sequence: maxStart, TimeMs: 0}
}

func streamToView(s *Stream) *StreamView {
	active := sortedActiveNames(s.ActiveRenditions)
	renditions := renditionsToView(s.Renditions)

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

func cutoverToView(s *Stream, cutoverID string, revision int64, idempotent bool) *CutoverView {
	return &CutoverView{
		StreamID:         s.ID,
		CutoverID:        cutoverID,
		Generation:       s.CurrentGeneration,
		Revision:         revision,
		Idempotent:       idempotent,
		ActiveRenditions: sortedActiveNames(s.ActiveRenditions),
		Renditions:       renditionsToView(s.Renditions),
		Watermark:        computeWatermark(s),
	}
}

func sortedActiveNames(active map[string]struct{}) []string {
	names := make([]string, 0, len(active))
	for name := range active {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func renditionsToView(renditions map[string]*RenditionState) map[string]RenditionView {
	view := make(map[string]RenditionView, len(renditions))
	for name, rs := range renditions {
		committed := rs.HeadSequence - rs.StartSequence
		if committed < 0 {
			committed = 0
		}
		buffered := len(rs.Segments) - int(committed)
		if buffered < 0 {
			buffered = 0
		}
		view[name] = RenditionView{
			StartSequence:    rs.StartSequence,
			HeadSequence:     rs.HeadSequence,
			ContiguousTimeMs: rs.ContiguousMs,
			BufferedSegments: buffered,
		}
	}
	return view
}

func copyStartMap(m map[string]int64) map[string]int64 {
	cp := make(map[string]int64, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
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

func validateCutoverInput(in CutoverInput) error {
	if in.StreamID == "" {
		return NewError(ErrCodeInvalidRequest, "streamId is required")
	}
	if in.CutoverID == "" {
		return NewError(ErrCodeInvalidRequest, "cutoverId is required")
	}
	if in.Generation < 0 {
		return NewError(ErrCodeInvalidRequest, "generation must be >= 0")
	}
	if in.ExpectedRevision < 0 {
		return NewError(ErrCodeInvalidRequest, "expectedRevision must be >= 0")
	}
	if len(in.Renditions) == 0 {
		return NewError(ErrCodeInvalidRequest, "renditions must not be empty")
	}
	for name, start := range in.Renditions {
		if name == "" {
			return NewError(ErrCodeInvalidRequest, "rendition name must not be empty")
		}
		if start < 0 {
			return NewError(ErrCodeInvalidRequest, fmt.Sprintf(
				"start sequence for rendition %q must be >= 0", name))
		}
	}
	return nil
}

func cutoverContentHash(generation, expectedRevision int64, renditions map[string]int64) string {
	names := make([]string, 0, len(renditions))
	for name := range renditions {
		names = append(names, name)
	}
	sort.Strings(names)
	h := sha256.New()
	fmt.Fprintf(h, "gen=%d;rev=%d;", generation, expectedRevision)
	for _, name := range names {
		fmt.Fprintf(h, "%s=%d;", name, renditions[name])
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
