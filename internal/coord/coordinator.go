package coord

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	maxNameLen       = 128
	maxBatchSegments = 10000
)

// Coordinator serializes all stream mutations behind a single mutex, so every
// request observes a linearizable order of commits and the revision returned
// in a response is exactly the revision at which that request committed.
type Coordinator struct {
	mu    sync.Mutex
	store *Store
}

func NewCoordinator(store *Store) *Coordinator {
	if store == nil {
		store = NewStore()
	}
	return &Coordinator{store: store}
}

// CreateStream registers a stream and declares its active renditions. The
// initial revision is 1.
func (c *Coordinator) CreateStream(streamID string, renditions []string) (Status, error) {
	if err := validateName("streamId", streamID); err != nil {
		return Status{}, err
	}
	if len(renditions) == 0 {
		return Status{}, errorf(CodeInvalidArgument, "renditions must declare at least one active rendition")
	}
	seen := make(map[string]struct{}, len(renditions))
	for _, r := range renditions {
		if err := validateName("rendition", r); err != nil {
			return Status{}, err
		}
		if _, dup := seen[r]; dup {
			return Status{}, errorf(CodeInvalidArgument, "duplicate rendition %q", r)
		}
		seen[r] = struct{}{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.store.get(streamID); ok {
		return Status{}, errorf(CodeAlreadyExists, "stream %q already exists", streamID)
	}
	st := &streamState{
		id:          streamID,
		generation:  0,
		revision:    1,
		renditions:  make(map[string]*renditionState, len(renditions)),
		submissions: make(map[string]submissionRecord),
		cutovers:    make(map[string]cutoverRecord),
	}
	for _, r := range renditions {
		st.renditions[r] = newRenditionState(0)
	}
	c.store.put(st)
	return statusOf(st), nil
}

// Ingest validates and atomically commits one batch of segments. A batch is
// all-or-nothing: any validation failure, stale generation, or content
// conflict leaves the stream untouched and the revision unadvanced.
func (c *Coordinator) Ingest(streamID string, req IngestRequest) (IngestResult, error) {
	if err := validateName("streamId", streamID); err != nil {
		return IngestResult{}, err
	}
	if err := validateName("rendition", req.Rendition); err != nil {
		return IngestResult{}, err
	}
	if req.SubmissionID == "" {
		return IngestResult{}, errorf(CodeInvalidArgument, "submissionId must not be empty")
	}
	if len(req.SubmissionID) > maxNameLen {
		return IngestResult{}, errorf(CodeInvalidArgument, "submissionId exceeds %d characters", maxNameLen)
	}
	if req.Generation < 0 {
		return IngestResult{}, errorf(CodeInvalidArgument, "generation must be >= 0")
	}
	segments, err := normalizeSegments(req.Segments)
	if err != nil {
		return IngestResult{}, err
	}
	req.Segments = segments

	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.store.get(streamID)
	if !ok {
		return IngestResult{}, errorf(CodeNotFound, "stream %q not found", streamID)
	}
	rend, ok := st.renditions[req.Rendition]
	if !ok {
		return IngestResult{}, errorf(CodeInvalidArgument, "rendition %q is not active on stream %q", req.Rendition, streamID)
	}

	// Idempotency is checked before the stale-generation gate so that a
	// retry of an already-committed batch stays a successful no-op even
	// after the stream has moved to a newer generation.
	key := submissionKey(req.Rendition, req.Generation, req.SubmissionID)
	fp := fingerprint(req)
	if rec, ok := st.submissions[key]; ok {
		if rec.fingerprint != fp {
			return IngestResult{}, errorf(CodeSubmissionConflict,
				"submissionId %q for rendition %q generation %d was already used with different content",
				req.SubmissionID, req.Rendition, req.Generation)
		}
		res := rec.result
		res.Replayed = true
		return res, nil
	}

	if req.Generation < st.generation {
		return IngestResult{}, errorf(CodeStaleGeneration,
			"generation %d is stale, stream %q is at generation %d", req.Generation, streamID, st.generation)
	}

	// Pre-commit conflict check: no state has been mutated yet, so
	// rejecting here keeps the batch all-or-nothing.
	if req.Generation == st.generation {
		for _, seg := range req.Segments {
			if existing, ok := rend.segments[seg.Sequence]; ok && existing != seg {
				return IngestResult{}, errorf(CodeSegmentConflict,
					"sequence %d of rendition %q already published with different content", seg.Sequence, req.Rendition)
			}
		}
	}

	// Commit point: everything below mutates state, nothing may fail.
	if req.Generation > st.generation {
		st.generation = req.Generation
		for _, r := range st.renditions {
			r.reset(0)
		}
		rend = st.renditions[req.Rendition]
	}
	applied := 0
	for _, seg := range req.Segments {
		if _, ok := rend.segments[seg.Sequence]; ok {
			continue
		}
		rend.segments[seg.Sequence] = seg
		applied++
	}
	for {
		if _, ok := rend.segments[rend.head+1]; !ok {
			break
		}
		rend.head++
	}
	st.revision++

	res := IngestResult{
		StreamID:   streamID,
		Rendition:  req.Rendition,
		Generation: req.Generation,
		Revision:   st.revision,
		Head:       rend.head,
		Watermark:  st.watermark(),
		Applied:    applied,
	}
	st.submissions[key] = submissionRecord{fingerprint: fp, result: res}
	return res, nil
}

// Status returns the current published state of a stream.
func (c *Coordinator) Status(streamID string) (Status, error) {
	if err := validateName("streamId", streamID); err != nil {
		return Status{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.store.get(streamID)
	if !ok {
		return Status{}, errorf(CodeNotFound, "stream %q not found", streamID)
	}
	return statusOf(st), nil
}

// Cutover atomically switches a stream to a higher producer generation with
// a newly declared active rendition set. It is guarded by expectedRevision
// (optimistic concurrency) and idempotent by cutoverId: an as-is retry of a
// committed cutover replays the original result instead of switching again.
// A failed cutover (stale revision, generation regression, invalid set)
// leaves the stream untouched and records nothing.
func (c *Coordinator) Cutover(streamID string, req CutoverRequest) (CutoverResult, error) {
	if err := validateName("streamId", streamID); err != nil {
		return CutoverResult{}, err
	}
	if req.CutoverID == "" {
		return CutoverResult{}, errorf(CodeInvalidArgument, "cutoverId must not be empty")
	}
	if len(req.CutoverID) > maxNameLen {
		return CutoverResult{}, errorf(CodeInvalidArgument, "cutoverId exceeds %d characters", maxNameLen)
	}
	if req.ExpectedRevision < 1 {
		return CutoverResult{}, errorf(CodeInvalidArgument, "expectedRevision must be >= 1")
	}
	if req.Generation < 0 {
		return CutoverResult{}, errorf(CodeInvalidArgument, "generation must be >= 0")
	}
	if err := validateResumeSet(req.Renditions); err != nil {
		return CutoverResult{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.store.get(streamID)
	if !ok {
		return CutoverResult{}, errorf(CodeNotFound, "stream %q not found", streamID)
	}

	// Idempotency is checked before the revision/generation gates so that a
	// retried switch whose response was lost still replays the committed
	// result — the gates would now reject it as stale.
	fp := cutoverFingerprint(req)
	if rec, ok := st.cutovers[req.CutoverID]; ok {
		if rec.fingerprint != fp {
			return CutoverResult{}, errorf(CodeCutoverConflict,
				"cutoverId %q was already used with different content", req.CutoverID)
		}
		res := rec.result
		res.Replayed = true
		return res, nil
	}

	if req.ExpectedRevision != st.revision {
		return CutoverResult{}, errorf(CodeRevisionConflict,
			"expectedRevision %d does not match current revision %d", req.ExpectedRevision, st.revision)
	}
	if req.Generation <= st.generation {
		return CutoverResult{}, errorf(CodeStaleGeneration,
			"generation %d does not advance current generation %d", req.Generation, st.generation)
	}

	// Commit point: replace the rendition set and generation atomically.
	// Everything from older generations — heads, buffered segments — is
	// discarded; only submission/cutover records survive for idempotency.
	st.generation = req.Generation
	rends := make(map[string]*renditionState, len(req.Renditions))
	for _, r := range req.Renditions {
		rends[r.Name] = newRenditionState(r.StartSequence)
	}
	st.renditions = rends
	st.revision++

	res := CutoverResult{
		StreamID:   streamID,
		CutoverID:  req.CutoverID,
		Generation: req.Generation,
		Revision:   st.revision,
		Watermark:  st.watermark(),
		Renditions: renditionStatuses(st),
	}
	st.cutovers[req.CutoverID] = cutoverRecord{fingerprint: fp, result: res}
	return res, nil
}

func renditionStatuses(st *streamState) map[string]RenditionStatus {
	rends := make(map[string]RenditionStatus, len(st.renditions))
	for name, r := range st.renditions {
		rends[name] = RenditionStatus{
			Head:     r.head,
			Buffered: len(r.segments) - int(r.head-r.start+1),
		}
	}
	return rends
}

func statusOf(st *streamState) Status {
	return Status{
		StreamID:   st.id,
		Revision:   st.revision,
		Generation: st.generation,
		Watermark:  st.watermark(),
		Renditions: renditionStatuses(st),
	}
}

func submissionKey(rendition string, generation int64, submissionID string) string {
	return rendition + "\x1f" + strconv.FormatInt(generation, 10) + "\x1f" + submissionID
}

// fingerprint hashes the canonical (sequence-ordered) content of a batch so
// that an as-is retry — even with segments listed in a different order — is
// recognized as the same submission, while any content change conflicts.
func fingerprint(req IngestRequest) string {
	segs := make([]Segment, len(req.Segments))
	copy(segs, req.Segments)
	sort.Slice(segs, func(i, j int) bool { return segs[i].Sequence < segs[j].Sequence })
	h := sha256.New()
	h.Write([]byte(req.Rendition))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(req.Generation, 10)))
	h.Write([]byte{0})
	for _, s := range segs {
		fmt.Fprintf(h, "%d:%d:%s\x00", s.Sequence, s.DurationMs, s.SHA256)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// cutoverFingerprint hashes the canonical (name-ordered) content of a
// cutover request so that an as-is retry is recognized while any content
// drift — including expectedRevision or resume points — conflicts.
func cutoverFingerprint(req CutoverRequest) string {
	rs := make([]RenditionResume, len(req.Renditions))
	copy(rs, req.Renditions)
	sort.Slice(rs, func(i, j int) bool { return rs[i].Name < rs[j].Name })
	h := sha256.New()
	h.Write([]byte(strconv.FormatInt(req.ExpectedRevision, 10)))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(req.Generation, 10)))
	h.Write([]byte{0})
	for _, r := range rs {
		fmt.Fprintf(h, "%s:%d\x00", r.Name, r.StartSequence)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func validateResumeSet(in []RenditionResume) error {
	if len(in) == 0 {
		return errorf(CodeInvalidArgument, "renditions must declare at least one active rendition")
	}
	seen := make(map[string]struct{}, len(in))
	for i, r := range in {
		if err := validateName("rendition", r.Name); err != nil {
			return err
		}
		if r.StartSequence < 0 {
			return errorf(CodeInvalidArgument, "renditions[%d]: startSequence must be >= 0", i)
		}
		if _, dup := seen[r.Name]; dup {
			return errorf(CodeInvalidArgument, "duplicate rendition %q", r.Name)
		}
		seen[r.Name] = struct{}{}
	}
	return nil
}

func validateName(field, v string) error {
	if v == "" {
		return errorf(CodeInvalidArgument, "%s must not be empty", field)
	}
	if len(v) > maxNameLen {
		return errorf(CodeInvalidArgument, "%s exceeds %d characters", field, maxNameLen)
	}
	for _, r := range v {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
			r == '-' || r == '_' || r == '.'
		if !ok {
			return errorf(CodeInvalidArgument, "%s %q contains invalid character %q", field, v, r)
		}
	}
	return nil
}

// normalizeSegments validates a batch, lowercases hashes, and drops exact
// in-batch duplicates. It returns an error before any state is touched.
func normalizeSegments(in []Segment) ([]Segment, error) {
	if len(in) == 0 {
		return nil, errorf(CodeInvalidArgument, "segments must not be empty")
	}
	if len(in) > maxBatchSegments {
		return nil, errorf(CodeInvalidArgument, "batch exceeds %d segments", maxBatchSegments)
	}
	out := make([]Segment, 0, len(in))
	seen := make(map[int64]Segment, len(in))
	for i, s := range in {
		if s.Sequence < 0 {
			return nil, errorf(CodeInvalidArgument, "segments[%d]: sequence must be >= 0", i)
		}
		if s.DurationMs <= 0 {
			return nil, errorf(CodeInvalidArgument, "segments[%d]: durationMs must be > 0", i)
		}
		sum, err := normalizeSHA256(s.SHA256)
		if err != nil {
			return nil, errorf(CodeInvalidArgument, "segments[%d]: %v", i, err)
		}
		s.SHA256 = sum
		if prev, ok := seen[s.Sequence]; ok {
			if prev != s {
				return nil, errorf(CodeInvalidArgument,
					"segments[%d]: sequence %d appears twice with different content", i, s.Sequence)
			}
			continue
		}
		seen[s.Sequence] = s
		out = append(out, s)
	}
	return out, nil
}

func normalizeSHA256(v string) (string, error) {
	if len(v) != 64 {
		return "", fmt.Errorf("sha256 must be 64 hex characters, got %d", len(v))
	}
	if _, err := hex.DecodeString(v); err != nil {
		return "", fmt.Errorf("sha256 must be hex encoded: %w", err)
	}
	return strings.ToLower(v), nil
}
