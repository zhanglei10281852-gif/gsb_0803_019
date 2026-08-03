package domain

import "testing"

func fillContiguous(t *testing.T, s *Stream, rendition string, gen uint64, from, to uint64) {
	t.Helper()
	for i := from; i <= to; i++ {
		if _, err := s.Submit(rendition, gen, rendition+"-g"+itoa(gen)+"-"+itoa(i), []Segment{seg(i, shaA)}); err != nil {
			t.Fatalf("fill %s gen %d seq %d: %v", rendition, gen, i, err)
		}
	}
}

func doCutover(s *Stream, id string, exp, gen uint64, splice map[string]uint64) (CutoverResult, error) {
	rends := make([]string, 0, len(splice))
	for r := range splice {
		rends = append(rends, r)
	}
	return s.Cutover(CutoverSpec{
		CutoverID:        id,
		ExpectedRevision: exp,
		Generation:       gen,
		Renditions:       rends,
		SplicePoints:     splice,
	})
}

func TestCutoverAdvancesGenerationAndSplice(t *testing.T) {
	s := mustStream(t, "720p")
	fillContiguous(t, s, "720p", 1, 0, 4) // head 4, revision 5
	rev := s.Snapshot().Revision

	res, err := doCutover(s, "co-1", rev, 2, map[string]uint64{"720p": 5})
	if err != nil {
		t.Fatalf("cutover: %v", err)
	}
	if !res.Applied {
		t.Fatal("cutover should apply")
	}
	snap := s.Snapshot()
	if snap.CurrentGeneration == nil || *snap.CurrentGeneration != 2 {
		t.Fatalf("current generation should be 2, got %v", snap.CurrentGeneration)
	}
	// New generation has no segments yet: head/watermark null, splice=5.
	rs := snap.Renditions["720p"]
	if rs.Head != nil {
		t.Fatalf("head should be nil right after cutover, got %v", *rs.Head)
	}
	if rs.SplicePoint == nil || *rs.SplicePoint != 5 {
		t.Fatalf("splice point should be 5, got %v", rs.SplicePoint)
	}
	if snap.Watermark != nil {
		t.Fatalf("watermark should be nil after cutover, got %v", *snap.Watermark)
	}
}

func TestNewGenerationStartsFromSpliceNotZero(t *testing.T) {
	s := mustStream(t, "720p")
	fillContiguous(t, s, "720p", 1, 0, 4)
	rev := s.Snapshot().Revision
	if _, err := doCutover(s, "co", rev, 2, map[string]uint64{"720p": 5}); err != nil {
		t.Fatal(err)
	}
	// Submitting seq 5 (the splice point) makes head=5 immediately.
	if _, err := s.Submit("720p", 2, "g2-5", []Segment{seg(5, shaA)}); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	if h := snap.Renditions["720p"].Head; h == nil || *h != 5 {
		t.Fatalf("head should be 5 (splice point), got %v", h)
	}
	if snap.Watermark == nil || *snap.Watermark != 5 {
		t.Fatalf("watermark should be 5, got %v", snap.Watermark)
	}
}

func TestNewGenerationCannotBorrowOldHeadOrBuffer(t *testing.T) {
	s := mustStream(t, "720p")
	fillContiguous(t, s, "720p", 1, 0, 4)
	rev := s.Snapshot().Revision
	if _, err := doCutover(s, "co", rev, 2, map[string]uint64{"720p": 10}); err != nil {
		t.Fatal(err)
	}
	// Submit seq 11 in gen 2 (past splice 10, hole at 10) -> buffered, no head.
	if _, err := s.Submit("720p", 2, "g2-11", []Segment{seg(11, shaA)}); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	rs := snap.Renditions["720p"]
	if rs.Head != nil {
		t.Fatalf("head must be nil: gen2 cannot borrow gen1 head, got %v", *rs.Head)
	}
	if len(rs.Buffered) != 1 || rs.Buffered[0] != 11 {
		t.Fatalf("buffered should be [11], got %v", rs.Buffered)
	}
	if snap.Watermark != nil {
		t.Fatalf("watermark must be nil, got %v", *snap.Watermark)
	}
}

func TestSegmentBelowSpliceRejected(t *testing.T) {
	s := mustStream(t, "720p")
	fillContiguous(t, s, "720p", 1, 0, 4)
	rev := s.Snapshot().Revision
	if _, err := doCutover(s, "co", rev, 2, map[string]uint64{"720p": 5}); err != nil {
		t.Fatal(err)
	}
	_, err := s.Submit("720p", 2, "bad", []Segment{seg(4, shaA)})
	if KindOf(err) != KindValidation {
		t.Fatalf("seq below splice should be validation error, got %v", err)
	}
}

func TestLateOldGenerationFenced(t *testing.T) {
	s := mustStream(t, "720p")
	fillContiguous(t, s, "720p", 1, 0, 4)
	rev := s.Snapshot().Revision
	if _, err := doCutover(s, "co", rev, 2, map[string]uint64{"720p": 5}); err != nil {
		t.Fatal(err)
	}
	revAfter := s.Snapshot().Revision

	// Late gen-1 report -> fenced, no side effects.
	_, err := s.Submit("720p", 1, "late", []Segment{seg(5, shaA)})
	if KindOf(err) != KindFenced {
		t.Fatalf("late old-gen report should be fenced, got %v", err)
	}
	if s.Snapshot().Revision != revAfter {
		t.Fatalf("fenced report must not change revision")
	}
	// The fenced submissionId must not be recorded: a retry is still fenced,
	// not reported as an idempotent success.
	_, err = s.Submit("720p", 1, "late", []Segment{seg(5, shaA)})
	if KindOf(err) != KindFenced {
		t.Fatalf("fenced retry should stay fenced, got %v", err)
	}
}

func TestCutoverIdempotentReplay(t *testing.T) {
	s := mustStream(t, "720p")
	fillContiguous(t, s, "720p", 1, 0, 4)
	rev := s.Snapshot().Revision
	r1, err := doCutover(s, "co", rev, 2, map[string]uint64{"720p": 5})
	if err != nil {
		t.Fatal(err)
	}
	// Verbatim retry (response lost): must NOT switch again, revision stable.
	r2, err := doCutover(s, "co", rev, 2, map[string]uint64{"720p": 5})
	if err != nil {
		t.Fatalf("replay should succeed idempotently, got %v", err)
	}
	if r2.Applied {
		t.Fatal("replay must not apply")
	}
	if r2.Revision != r1.Revision {
		t.Fatalf("replay revision %d != original %d", r2.Revision, r1.Revision)
	}
}

func TestCutoverContentDriftConflicts(t *testing.T) {
	s := mustStream(t, "720p")
	fillContiguous(t, s, "720p", 1, 0, 4)
	rev := s.Snapshot().Revision
	if _, err := doCutover(s, "co", rev, 2, map[string]uint64{"720p": 5}); err != nil {
		t.Fatal(err)
	}
	// Same cutoverId, different splice point -> conflict.
	_, err := doCutover(s, "co", rev, 2, map[string]uint64{"720p": 6})
	if KindOf(err) != KindConflict {
		t.Fatalf("content drift should conflict, got %v", err)
	}
}

func TestCutoverStaleExpectedRevisionConflicts(t *testing.T) {
	s := mustStream(t, "720p")
	fillContiguous(t, s, "720p", 1, 0, 4)
	// expectedRevision is stale (0, but actual is 5).
	_, err := doCutover(s, "co", 0, 2, map[string]uint64{"720p": 5})
	if KindOf(err) != KindConflict {
		t.Fatalf("stale expectedRevision should conflict, got %v", err)
	}
	if s.Snapshot().CurrentGeneration == nil || *s.Snapshot().CurrentGeneration != 1 {
		t.Fatalf("stream should remain on generation 1")
	}
}

func TestCutoverGenerationRegressionConflicts(t *testing.T) {
	s := mustStream(t, "720p")
	fillContiguous(t, s, "720p", 1, 0, 4)
	rev := s.Snapshot().Revision
	// Cutover to same generation (1) -> not advancing -> conflict.
	if _, err := doCutover(s, "co", rev, 1, map[string]uint64{"720p": 5}); KindOf(err) != KindConflict {
		t.Fatalf("non-advancing generation should conflict, got %v", err)
	}
}

func TestCutoverInvalidRenditionSetConflicts(t *testing.T) {
	s := mustStream(t, "720p")
	fillContiguous(t, s, "720p", 1, 0, 4)
	rev := s.Snapshot().Revision
	// Missing splice point for declared rendition.
	_, err := s.Cutover(CutoverSpec{
		CutoverID: "co", ExpectedRevision: rev, Generation: 2,
		Renditions: []string{"720p", "1080p"}, SplicePoints: map[string]uint64{"720p": 5},
	})
	if KindOf(err) != KindConflict {
		t.Fatalf("missing splice point should conflict, got %v", err)
	}
	// Empty rendition set.
	_, err = s.Cutover(CutoverSpec{
		CutoverID: "co2", ExpectedRevision: rev, Generation: 2,
		Renditions: nil, SplicePoints: nil,
	})
	if KindOf(err) != KindConflict {
		t.Fatalf("empty rendition set should conflict, got %v", err)
	}
}

func TestCutoverChangesActiveRenditions(t *testing.T) {
	s := mustStream(t, "720p", "1080p")
	fillContiguous(t, s, "720p", 1, 0, 2)
	fillContiguous(t, s, "1080p", 1, 0, 2)
	rev := s.Snapshot().Revision
	// New generation drops 1080p, adds 480p.
	if _, err := doCutover(s, "co", rev, 2, map[string]uint64{"720p": 3, "480p": 0}); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	if len(snap.ActiveRenditions) != 2 {
		t.Fatalf("expected 2 active renditions, got %v", snap.ActiveRenditions)
	}
	if _, ok := snap.Renditions["1080p"]; ok {
		t.Fatal("1080p should no longer be active")
	}
	if _, ok := snap.Renditions["480p"]; !ok {
		t.Fatal("480p should be active")
	}
	// A gen-2 report for the dropped 1080p is fenced/invalid, never applied.
	_, err := s.Submit("1080p", 2, "x", []Segment{seg(0, shaA)})
	if KindOf(err) == KindValidation {
		return // rendition not active for gen 2 -> validation is acceptable
	}
	t.Fatalf("expected validation error for dropped rendition, got %v", err)
}

func TestWatermarkAcrossNewGenerationRenditions(t *testing.T) {
	s := mustStream(t, "a", "b")
	fillContiguous(t, s, "a", 1, 0, 2)
	fillContiguous(t, s, "b", 1, 0, 2)
	rev := s.Snapshot().Revision
	if _, err := doCutover(s, "co", rev, 2, map[string]uint64{"a": 3, "b": 10}); err != nil {
		t.Fatal(err)
	}
	// a contiguous 3..5, b contiguous 10..10 -> watermark = min(5,10)=5.
	fillContiguous(t, s, "a", 2, 3, 5)
	fillContiguous(t, s, "b", 2, 10, 10)
	snap := s.Snapshot()
	if snap.Watermark == nil || *snap.Watermark != 5 {
		t.Fatalf("watermark should be 5, got %v", snap.Watermark)
	}
	// Only one rendition advanced past splice -> before b arrives watermark nil.
	s2 := mustStream(t, "a", "b")
	fillContiguous(t, s2, "a", 1, 0, 2)
	fillContiguous(t, s2, "b", 1, 0, 2)
	rev2 := s2.Snapshot().Revision
	doCutover(s2, "co", rev2, 2, map[string]uint64{"a": 3, "b": 10})
	fillContiguous(t, s2, "a", 2, 3, 5) // only a ready
	if w := s2.Snapshot().Watermark; w != nil {
		t.Fatalf("watermark should be nil until all new-gen renditions ready, got %v", *w)
	}
}
