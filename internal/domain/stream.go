// Package domain contains the pure coordination logic for live segment
// publishing. It has no knowledge of HTTP, storage engines or concurrency:
// every method on Stream assumes the caller holds exclusive access to that
// Stream (the storage layer provides a per-stream lock). Keeping the logic
// single-threaded here is what lets us reason about ordering and atomicity.
package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Kind classifies a domain error so the HTTP layer can pick a status code
// without string matching.
type Kind int

const (
	// KindValidation - the request was malformed or violates an invariant.
	KindValidation Kind = iota
	// KindConflict - the request collides with already-committed state
	// (same submissionId with different content, or a sequence rewritten
	// with different bytes).
	KindConflict
	// KindNotFound - the referenced stream does not exist.
	KindNotFound
	// KindAlreadyExists - a stream with a different declaration already exists.
	KindAlreadyExists
)

// Error is a typed domain error carrying a Kind.
type Error struct {
	Kind Kind
	Msg  string
}

func (e *Error) Error() string { return e.Msg }

func newErr(k Kind, format string, args ...any) *Error {
	return &Error{Kind: k, Msg: fmt.Sprintf(format, args...)}
}

// KindOf extracts the Kind from an error, defaulting to KindValidation.
func KindOf(err error) Kind {
	var de *Error
	if errors.As(err, &de) {
		return de.Kind
	}
	return KindValidation
}

// Segment is a single transcoded chunk reported by a producer.
type Segment struct {
	Sequence   uint64
	DurationMs uint64
	SHA256     string
}

// SubmitResult reports the outcome of a submission.
type SubmitResult struct {
	// Revision is the stream revision observed after this call. For an
	// idempotent replay it equals the revision recorded when the batch was
	// first accepted's successor state, i.e. the current revision.
	Revision uint64
	// Applied is true when this call mutated the stream. It is false for an
	// exact idempotent replay of a previously accepted submission.
	Applied bool
}

// genState holds every segment reported for one (rendition, generation) pair
// plus its incrementally maintained contiguous head.
type genState struct {
	segments map[uint64]Segment
	headSet  bool
	head     uint64
}

func newGenState() *genState {
	return &genState{segments: make(map[uint64]Segment)}
}

// advance moves head forward across any newly-contiguous sequences. The head
// is the largest H such that sequences 0..H are all present; it never jumps a
// hole.
func (g *genState) advance() {
	var next uint64
	if g.headSet {
		next = g.head + 1
	} else {
		if _, ok := g.segments[0]; !ok {
			return
		}
		g.head = 0
		g.headSet = true
		next = 1
	}
	for {
		if _, ok := g.segments[next]; !ok {
			return
		}
		g.head = next
		if next == ^uint64(0) {
			return
		}
		next++
	}
}

// buffered returns the sorted sequences held past the contiguous head (the
// out-of-order backlog waiting for a hole to fill).
func (g *genState) buffered() []uint64 {
	var out []uint64
	for seq := range g.segments {
		if !g.headSet || seq > g.head {
			out = append(out, seq)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Stream is the aggregate root. All state changes go through its methods.
type Stream struct {
	id          string
	active      map[string]struct{}
	activeOrder []string

	revision uint64

	hasGen bool
	maxGen uint64

	// store[rendition][generation] -> genState
	store map[string]map[uint64]*genState

	// submissions maps an idempotency key to the content fingerprint of the
	// batch that was accepted under it.
	submissions map[string]string
}

// NewStream validates the declaration and constructs an empty stream.
func NewStream(id string, renditions []string) (*Stream, error) {
	if strings.TrimSpace(id) == "" {
		return nil, newErr(KindValidation, "streamId must not be empty")
	}
	if len(renditions) == 0 {
		return nil, newErr(KindValidation, "at least one active rendition is required")
	}
	active := make(map[string]struct{}, len(renditions))
	order := make([]string, 0, len(renditions))
	for _, r := range renditions {
		if strings.TrimSpace(r) == "" {
			return nil, newErr(KindValidation, "rendition names must not be empty")
		}
		if _, dup := active[r]; dup {
			return nil, newErr(KindValidation, "duplicate rendition %q in declaration", r)
		}
		active[r] = struct{}{}
		order = append(order, r)
	}
	sort.Strings(order)
	return &Stream{
		id:          id,
		active:      active,
		activeOrder: order,
		store:       make(map[string]map[uint64]*genState),
		submissions: make(map[string]string),
	}, nil
}

// ID returns the stream identifier.
func (s *Stream) ID() string { return s.id }

// SameRenditions reports whether the given set matches this stream's
// declaration, used to make stream creation idempotent.
func (s *Stream) SameRenditions(renditions []string) bool {
	if len(renditions) != len(s.active) {
		return false
	}
	for _, r := range renditions {
		if _, ok := s.active[r]; !ok {
			return false
		}
	}
	return true
}

func submissionKey(rendition string, generation uint64, submissionID string) string {
	return fmt.Sprintf("%s\x00%d\x00%s", rendition, generation, submissionID)
}

// fingerprint produces a stable content hash of a batch, independent of the
// order in which segments were listed.
func fingerprint(segs []Segment) string {
	cp := make([]Segment, len(segs))
	copy(cp, segs)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Sequence < cp[j].Sequence })
	h := sha256.New()
	for _, seg := range cp {
		fmt.Fprintf(h, "%d:%d:%s\n", seg.Sequence, seg.DurationMs, strings.ToLower(seg.SHA256))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func validSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// Submit applies a batch under an idempotency key. It is all-or-nothing: every
// segment is validated and every conflict detected before any state changes,
// so a rejected batch leaves no partial writes.
func (s *Stream) Submit(rendition string, generation uint64, submissionID string, segs []Segment) (SubmitResult, error) {
	// --- validation (no mutation) ---
	if strings.TrimSpace(submissionID) == "" {
		return SubmitResult{}, newErr(KindValidation, "submissionId must not be empty")
	}
	if _, ok := s.active[rendition]; !ok {
		return SubmitResult{}, newErr(KindValidation, "rendition %q is not active for this stream", rendition)
	}
	if len(segs) == 0 {
		return SubmitResult{}, newErr(KindValidation, "a submission must contain at least one segment")
	}
	seen := make(map[uint64]struct{}, len(segs))
	for _, seg := range segs {
		if seg.DurationMs == 0 {
			return SubmitResult{}, newErr(KindValidation, "segment %d has non-positive durationMs", seg.Sequence)
		}
		if !validSHA256(seg.SHA256) {
			return SubmitResult{}, newErr(KindValidation, "segment %d has an invalid sha256", seg.Sequence)
		}
		if _, dup := seen[seg.Sequence]; dup {
			return SubmitResult{}, newErr(KindValidation, "duplicate sequence %d within submission", seg.Sequence)
		}
		seen[seg.Sequence] = struct{}{}
	}

	// --- idempotency / conflict on the submissionId ---
	key := submissionKey(rendition, generation, submissionID)
	fp := fingerprint(segs)
	if prev, ok := s.submissions[key]; ok {
		if prev == fp {
			// Exact replay: no state change, revision unchanged.
			return SubmitResult{Revision: s.revision, Applied: false}, nil
		}
		return SubmitResult{}, newErr(KindConflict,
			"submissionId %q already used for a different batch on rendition %q generation %d",
			submissionID, rendition, generation)
	}

	// --- conflict against already-committed segments (no mutation yet) ---
	gs := s.genStateFor(rendition, generation, false)
	if gs != nil {
		for _, seg := range segs {
			if existing, ok := gs.segments[seg.Sequence]; ok {
				if existing.DurationMs != seg.DurationMs || !strings.EqualFold(existing.SHA256, seg.SHA256) {
					return SubmitResult{}, newErr(KindConflict,
						"sequence %d on rendition %q generation %d already committed with different content",
						seg.Sequence, rendition, generation)
				}
			}
		}
	}

	// --- commit (all checks passed) ---
	gs = s.genStateFor(rendition, generation, true)
	for _, seg := range segs {
		gs.segments[seg.Sequence] = Segment{
			Sequence:   seg.Sequence,
			DurationMs: seg.DurationMs,
			SHA256:     strings.ToLower(seg.SHA256),
		}
	}
	gs.advance()

	if !s.hasGen || generation > s.maxGen {
		s.hasGen = true
		s.maxGen = generation
	}
	s.submissions[key] = fp
	s.revision++

	return SubmitResult{Revision: s.revision, Applied: true}, nil
}

func (s *Stream) genStateFor(rendition string, generation uint64, create bool) *genState {
	byGen := s.store[rendition]
	if byGen == nil {
		if !create {
			return nil
		}
		byGen = make(map[uint64]*genState)
		s.store[rendition] = byGen
	}
	gs := byGen[generation]
	if gs == nil && create {
		gs = newGenState()
		byGen[generation] = gs
	}
	return gs
}

// --- read model ---

// RenditionStatus is the per-rendition view within the current generation.
type RenditionStatus struct {
	Generation   *uint64
	Head         *uint64
	SegmentCount int
	Buffered     []uint64
}

// Snapshot is an immutable read model of the stream.
type Snapshot struct {
	StreamID          string
	Revision          uint64
	ActiveRenditions  []string
	CurrentGeneration *uint64
	Renditions        map[string]RenditionStatus
	Watermark         *uint64
}

// Snapshot builds the current read model. Per-rendition status and the
// watermark are all scoped to the current (highest observed) generation.
func (s *Stream) Snapshot() Snapshot {
	snap := Snapshot{
		StreamID:         s.id,
		Revision:         s.revision,
		ActiveRenditions: append([]string(nil), s.activeOrder...),
		Renditions:       make(map[string]RenditionStatus, len(s.activeOrder)),
	}

	var curGen uint64
	if s.hasGen {
		curGen = s.maxGen
		g := curGen
		snap.CurrentGeneration = &g
	}

	allContiguous := s.hasGen
	var watermark uint64
	first := true

	for _, r := range s.activeOrder {
		rs := RenditionStatus{}
		if s.hasGen {
			gs := s.genStateFor(r, curGen, false)
			if gs != nil {
				g := curGen
				rs.Generation = &g
				rs.SegmentCount = len(gs.segments)
				rs.Buffered = gs.buffered()
				if gs.headSet {
					h := gs.head
					rs.Head = &h
					if first || h < watermark {
						watermark = h
						first = false
					}
				} else {
					allContiguous = false
				}
			} else {
				allContiguous = false
			}
		}
		snap.Renditions[r] = rs
	}

	if allContiguous && !first {
		w := watermark
		snap.Watermark = &w
	}
	return snap
}
