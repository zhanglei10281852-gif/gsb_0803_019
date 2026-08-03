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
	// KindFenced - a report belongs to a generation that has been superseded
	// by a cutover; it is rejected with no side effects.
	KindFenced
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
// plus its incrementally maintained contiguous head. The head is measured from
// base, the generation's splice point for this rendition: sequences below base
// belong to an earlier generation and are never counted here, so a new
// generation cannot borrow an old generation's head or buffered segments.
type genState struct {
	base     uint64
	segments map[uint64]Segment
	headSet  bool
	head     uint64
}

func newGenState(base uint64) *genState {
	return &genState{base: base, segments: make(map[uint64]Segment)}
}

// advance moves head forward across any newly-contiguous sequences. The head
// is the largest H such that sequences base..H are all present; it never jumps
// a hole and never precedes base.
func (g *genState) advance() {
	var next uint64
	if g.headSet {
		next = g.head + 1
	} else {
		if _, ok := g.segments[g.base]; !ok {
			return
		}
		g.head = g.base
		g.headSet = true
		if g.base == ^uint64(0) {
			return
		}
		next = g.base + 1
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

// generation describes one producer generation: which renditions are active
// and, for each, the sequence at which this generation splices in (its base).
// Generations are created implicitly by the first submit before any cutover
// (base 0, the stream's declared renditions) or explicitly by a cutover.
type generation struct {
	number uint64
	active map[string]struct{}
	order  []string
	base   map[string]uint64
}

// Stream is the aggregate root. All state changes go through its methods.
type Stream struct {
	id          string
	active      map[string]struct{}
	activeOrder []string

	revision uint64

	hasGen bool
	curGen uint64
	// cutoverPerformed becomes true after the first explicit cutover. Only
	// then is generation fencing enforced; before it the stream behaves
	// exactly as in the pre-cutover contract.
	cutoverPerformed bool

	// gens holds the declaration (active set + splice bases) of every
	// generation seen so far.
	gens map[uint64]*generation

	// store[rendition][generation] -> genState
	store map[string]map[uint64]*genState

	// submissions maps an idempotency key to the content fingerprint of the
	// batch that was accepted under it.
	submissions map[string]string

	// cutovers maps a cutoverId to the outcome recorded when it was first
	// accepted, making a lost-response retry idempotent.
	cutovers map[string]cutoverRecord
}

// cutoverRecord captures a completed cutover for idempotent replay and
// content-drift detection.
type cutoverRecord struct {
	fingerprint string
	revision    uint64
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
		gens:        make(map[uint64]*generation),
		store:       make(map[string]map[uint64]*genState),
		submissions: make(map[string]string),
		cutovers:    make(map[string]cutoverRecord),
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

// genDescriptor is the effective active-set and splice bases for a generation,
// computed without mutating the stream. Before any cutover an unseen
// generation defaults to the declared renditions spliced at sequence 0, which
// reproduces the pre-cutover contract exactly.
type genDescriptor struct {
	active map[string]struct{}
	order  []string
	base   map[string]uint64
}

func (s *Stream) descriptorFor(generation uint64) genDescriptor {
	if g, ok := s.gens[generation]; ok {
		return genDescriptor{active: g.active, order: g.order, base: g.base}
	}
	return genDescriptor{active: s.active, order: s.activeOrder, base: nil}
}

func (d genDescriptor) baseOf(rendition string) uint64 {
	if d.base == nil {
		return 0
	}
	return d.base[rendition]
}

// Submit applies a batch under an idempotency key. It is all-or-nothing: every
// segment is validated and every conflict detected before any state changes,
// so a rejected batch leaves no partial writes. Once a cutover has occurred,
// any report for a superseded generation is fenced with no side effects.
func (s *Stream) Submit(rendition string, generation uint64, submissionID string, segs []Segment) (SubmitResult, error) {
	// --- structural validation (no mutation) ---
	if strings.TrimSpace(submissionID) == "" {
		return SubmitResult{}, newErr(KindValidation, "submissionId must not be empty")
	}

	// --- fencing: superseded generations fail with no side effects ---
	// This is checked before idempotency so a late replay of an old-generation
	// batch is fenced rather than reported as an idempotent success, and it
	// never touches revision, idempotency records or the watermark.
	_, known := s.gens[generation]
	if s.cutoverPerformed {
		if generation < s.curGen {
			return SubmitResult{}, newErr(KindFenced,
				"generation %d has been superseded by generation %d", generation, s.curGen)
		}
		if !known {
			// curGen is always a known generation, so an unknown generation
			// here is strictly greater than the current one: it can only be
			// established through a cutover.
			return SubmitResult{}, newErr(KindConflict,
				"generation %d is not established; perform a cutover to advance", generation)
		}
	}

	desc := s.descriptorFor(generation)
	if _, ok := desc.active[rendition]; !ok {
		return SubmitResult{}, newErr(KindValidation,
			"rendition %q is not active for generation %d", rendition, generation)
	}
	base := desc.baseOf(rendition)

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
		if seg.Sequence < base {
			return SubmitResult{}, newErr(KindValidation,
				"sequence %d precedes generation %d splice point %d for rendition %q",
				seg.Sequence, generation, base, rendition)
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
	gs := s.genStateFor(rendition, generation, false, base)
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
	if !known {
		// Pre-cutover implicit generation: declared renditions spliced at 0.
		g := implicitGeneration(s, generation)
		s.gens[generation] = &g
	}
	gs = s.genStateFor(rendition, generation, true, base)
	for _, seg := range segs {
		gs.segments[seg.Sequence] = Segment{
			Sequence:   seg.Sequence,
			DurationMs: seg.DurationMs,
			SHA256:     strings.ToLower(seg.SHA256),
		}
	}
	gs.advance()

	if !s.hasGen || generation > s.curGen {
		s.hasGen = true
		s.curGen = generation
	}
	s.submissions[key] = fp
	s.revision++

	return SubmitResult{Revision: s.revision, Applied: true}, nil
}

// implicitGeneration builds an implicit pre-cutover generation record: the
// declared active renditions, all spliced at sequence 0.
func implicitGeneration(s *Stream, number uint64) generation {
	base := make(map[string]uint64, len(s.activeOrder))
	for _, r := range s.activeOrder {
		base[r] = 0
	}
	return generation{
		number: number,
		active: s.active,
		order:  append([]string(nil), s.activeOrder...),
		base:   base,
	}
}

func (s *Stream) genStateFor(rendition string, generation uint64, create bool, base uint64) *genState {
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
		gs = newGenState(base)
		byGen[generation] = gs
	}
	return gs
}

// CutoverSpec declares a lossless active/standby switch.
type CutoverSpec struct {
	CutoverID        string
	ExpectedRevision uint64
	Generation       uint64
	Renditions       []string
	SplicePoints     map[string]uint64
}

// CutoverResult reports the outcome of a cutover.
type CutoverResult struct {
	Revision uint64
	Applied  bool
}

// cutoverFingerprint is a stable content hash of a cutover request, used to
// distinguish a verbatim retry from a drifting reuse of the same cutoverId.
func cutoverFingerprint(spec CutoverSpec) string {
	rends := append([]string(nil), spec.Renditions...)
	sort.Strings(rends)
	h := sha256.New()
	fmt.Fprintf(h, "gen=%d\n", spec.Generation)
	fmt.Fprintf(h, "exp=%d\n", spec.ExpectedRevision)
	for _, r := range rends {
		fmt.Fprintf(h, "r=%s:%d\n", r, spec.SplicePoints[r])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Cutover atomically switches the stream to a higher producer generation with a
// new active rendition set and per-rendition splice points. It is guarded by
// expectedRevision (optimistic concurrency) and made idempotent by cutoverId.
// After it commits, reports for any earlier generation are fenced.
func (s *Stream) Cutover(spec CutoverSpec) (CutoverResult, error) {
	if strings.TrimSpace(spec.CutoverID) == "" {
		return CutoverResult{}, newErr(KindValidation, "cutoverId must not be empty")
	}

	fp := cutoverFingerprint(spec)

	// --- idempotency FIRST, before expectedRevision. A lost-response retry
	// arrives after revision has already advanced, so an expectedRevision
	// check would wrongly reject it; the cutoverId replay short-circuits. ---
	if rec, ok := s.cutovers[spec.CutoverID]; ok {
		if rec.fingerprint == fp {
			return CutoverResult{Revision: rec.revision, Applied: false}, nil
		}
		return CutoverResult{}, newErr(KindConflict,
			"cutoverId %q already used for a different cutover", spec.CutoverID)
	}

	// --- optimistic concurrency: stale expectedRevision is a stable conflict. ---
	if spec.ExpectedRevision != s.revision {
		return CutoverResult{}, newErr(KindConflict,
			"expectedRevision %d does not match current revision %d",
			spec.ExpectedRevision, s.revision)
	}

	// --- generation must move strictly forward. ---
	if s.hasGen && spec.Generation <= s.curGen {
		return CutoverResult{}, newErr(KindConflict,
			"cutover generation %d does not advance past current generation %d",
			spec.Generation, s.curGen)
	}

	// --- validate the new rendition set and its splice points. An invalid
	// set is a stable conflict, consistent with the other cutover guards. ---
	if len(spec.Renditions) == 0 {
		return CutoverResult{}, newErr(KindConflict, "cutover must declare at least one active rendition")
	}
	active := make(map[string]struct{}, len(spec.Renditions))
	order := make([]string, 0, len(spec.Renditions))
	base := make(map[string]uint64, len(spec.Renditions))
	for _, r := range spec.Renditions {
		if strings.TrimSpace(r) == "" {
			return CutoverResult{}, newErr(KindConflict, "cutover rendition names must not be empty")
		}
		if _, dup := active[r]; dup {
			return CutoverResult{}, newErr(KindConflict, "duplicate rendition %q in cutover", r)
		}
		sp, ok := spec.SplicePoints[r]
		if !ok {
			return CutoverResult{}, newErr(KindConflict, "cutover is missing a splice point for rendition %q", r)
		}
		active[r] = struct{}{}
		order = append(order, r)
		base[r] = sp
	}
	if len(spec.SplicePoints) != len(active) {
		return CutoverResult{}, newErr(KindConflict, "cutover splice points do not match the declared rendition set")
	}
	sort.Strings(order)

	// --- commit ---
	s.gens[spec.Generation] = &generation{
		number: spec.Generation,
		active: active,
		order:  order,
		base:   base,
	}
	// Seed empty per-rendition state at the splice base so heads for the new
	// generation are measured from the splice point, never borrowing an old
	// generation's head or buffered segments.
	for _, r := range order {
		s.genStateFor(r, spec.Generation, true, base[r])
	}
	s.hasGen = true
	s.curGen = spec.Generation
	s.cutoverPerformed = true
	s.revision++
	s.cutovers[spec.CutoverID] = cutoverRecord{fingerprint: fp, revision: s.revision}

	return CutoverResult{Revision: s.revision, Applied: true}, nil
}

// --- read model ---

// RenditionStatus is the per-rendition view within the current generation.
type RenditionStatus struct {
	Generation   *uint64
	SplicePoint  *uint64
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

// Snapshot builds the current read model. Active renditions, per-rendition
// status and the watermark are all scoped to the current generation: after a
// cutover this is the new generation's declared set, measured from its splice
// points, so no old-generation head or backlog leaks into the read model.
func (s *Stream) Snapshot() Snapshot {
	snap := Snapshot{
		StreamID:   s.id,
		Revision:   s.revision,
		Renditions: make(map[string]RenditionStatus),
	}

	// Before any submit the current generation's active set is the stream's
	// original declaration; afterwards it is the current generation's set.
	order := s.activeOrder
	var desc genDescriptor
	if s.hasGen {
		curGen := s.curGen
		g := curGen
		snap.CurrentGeneration = &g
		desc = s.descriptorFor(curGen)
		order = desc.order
	}
	snap.ActiveRenditions = append([]string(nil), order...)

	allContiguous := s.hasGen
	var watermark uint64
	first := true

	for _, r := range order {
		rs := RenditionStatus{}
		if s.hasGen {
			curGen := s.curGen
			g := curGen
			rs.Generation = &g
			b := desc.baseOf(r)
			rs.SplicePoint = &b
			gs := s.genStateFor(r, curGen, false, b)
			if gs != nil {
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
