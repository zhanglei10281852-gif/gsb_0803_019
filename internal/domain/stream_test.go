package domain

import "testing"

const shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func seg(seq uint64, sha string) Segment {
	return Segment{Sequence: seq, DurationMs: 1000, SHA256: sha}
}

func mustStream(t *testing.T, rends ...string) *Stream {
	t.Helper()
	s, err := NewStream("s1", rends)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	return s
}

func TestNewStreamValidation(t *testing.T) {
	if _, err := NewStream("", []string{"720p"}); KindOf(err) != KindValidation {
		t.Fatalf("empty id should be validation error, got %v", err)
	}
	if _, err := NewStream("s", nil); KindOf(err) != KindValidation {
		t.Fatalf("no renditions should be validation error, got %v", err)
	}
	if _, err := NewStream("s", []string{"a", "a"}); KindOf(err) != KindValidation {
		t.Fatalf("duplicate rendition should be validation error, got %v", err)
	}
}

func TestHeadDoesNotCrossHole(t *testing.T) {
	s := mustStream(t, "720p")
	// Submit 0, then 2 (skipping 1). Head must stay at 0, 2 buffered.
	if _, err := s.Submit("720p", 1, "sub-0", []Segment{seg(0, shaA)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit("720p", 1, "sub-2", []Segment{seg(2, shaA)}); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	rs := snap.Renditions["720p"]
	if rs.Head == nil || *rs.Head != 0 {
		t.Fatalf("head should be 0, got %v", rs.Head)
	}
	if len(rs.Buffered) != 1 || rs.Buffered[0] != 2 {
		t.Fatalf("expected [2] buffered, got %v", rs.Buffered)
	}
	// Fill the hole: head should jump to 2.
	if _, err := s.Submit("720p", 1, "sub-1", []Segment{seg(1, shaA)}); err != nil {
		t.Fatal(err)
	}
	snap = s.Snapshot()
	if h := snap.Renditions["720p"].Head; h == nil || *h != 2 {
		t.Fatalf("head should advance to 2, got %v", h)
	}
}

func TestHeadNilWhenZeroMissing(t *testing.T) {
	s := mustStream(t, "720p")
	if _, err := s.Submit("720p", 1, "sub-1", []Segment{seg(1, shaA)}); err != nil {
		t.Fatal(err)
	}
	if h := s.Snapshot().Renditions["720p"].Head; h != nil {
		t.Fatalf("head should be nil when seq 0 missing, got %v", *h)
	}
}

func TestWatermarkMinAcrossActive(t *testing.T) {
	s := mustStream(t, "720p", "1080p")
	// 720p contiguous to 2, 1080p contiguous to 1 -> watermark 1.
	for i := uint64(0); i <= 2; i++ {
		if _, err := s.Submit("720p", 5, "a"+itoa(i), []Segment{seg(i, shaA)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := uint64(0); i <= 1; i++ {
		if _, err := s.Submit("1080p", 5, "b"+itoa(i), []Segment{seg(i, shaA)}); err != nil {
			t.Fatal(err)
		}
	}
	snap := s.Snapshot()
	if snap.Watermark == nil || *snap.Watermark != 1 {
		t.Fatalf("watermark should be 1, got %v", snap.Watermark)
	}
}

func TestWatermarkNilUntilAllActiveHaveHead(t *testing.T) {
	s := mustStream(t, "720p", "1080p")
	if _, err := s.Submit("720p", 1, "a0", []Segment{seg(0, shaA)}); err != nil {
		t.Fatal(err)
	}
	// 1080p has nothing in gen 1 -> watermark must be nil.
	if w := s.Snapshot().Watermark; w != nil {
		t.Fatalf("watermark should be nil, got %v", *w)
	}
}

func TestWatermarkScopedToCurrentGeneration(t *testing.T) {
	s := mustStream(t, "720p", "1080p")
	// generation 1 fully contiguous for both.
	for _, r := range []string{"720p", "1080p"} {
		if _, err := s.Submit(r, 1, r+"g1", []Segment{seg(0, shaA)}); err != nil {
			t.Fatal(err)
		}
	}
	if w := s.Snapshot().Watermark; w == nil || *w != 0 {
		t.Fatalf("gen1 watermark should be 0, got %v", w)
	}
	// New generation 2 only has 720p -> current gen is 2, watermark nil.
	if _, err := s.Submit("720p", 2, "g2-720", []Segment{seg(0, shaA)}); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	if snap.CurrentGeneration == nil || *snap.CurrentGeneration != 2 {
		t.Fatalf("current gen should be 2, got %v", snap.CurrentGeneration)
	}
	if snap.Watermark != nil {
		t.Fatalf("watermark should reset to nil for new generation, got %v", *snap.Watermark)
	}
}

func TestIdempotentReplayNoRevisionBump(t *testing.T) {
	s := mustStream(t, "720p")
	r1, err := s.Submit("720p", 1, "sub", []Segment{seg(0, shaA), seg(1, shaA)})
	if err != nil {
		t.Fatal(err)
	}
	if !r1.Applied {
		t.Fatal("first submit should apply")
	}
	// Replay with segments in a different order -> same content.
	r2, err := s.Submit("720p", 1, "sub", []Segment{seg(1, shaA), seg(0, shaA)})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Applied {
		t.Fatal("replay should not apply")
	}
	if r2.Revision != r1.Revision {
		t.Fatalf("replay must not bump revision: %d vs %d", r2.Revision, r1.Revision)
	}
}

func TestSameSubmissionDifferentContentConflicts(t *testing.T) {
	s := mustStream(t, "720p")
	if _, err := s.Submit("720p", 1, "sub", []Segment{seg(0, shaA)}); err != nil {
		t.Fatal(err)
	}
	_, err := s.Submit("720p", 1, "sub", []Segment{seg(0, shaB)})
	if KindOf(err) != KindConflict {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestSequenceRewriteConflict(t *testing.T) {
	s := mustStream(t, "720p")
	if _, err := s.Submit("720p", 1, "sub1", []Segment{seg(0, shaA)}); err != nil {
		t.Fatal(err)
	}
	// Different submissionId, same sequence, different bytes -> conflict.
	_, err := s.Submit("720p", 1, "sub2", []Segment{seg(0, shaB)})
	if KindOf(err) != KindConflict {
		t.Fatalf("expected conflict on rewrite, got %v", err)
	}
}

func TestBatchAtomicityOnConflict(t *testing.T) {
	s := mustStream(t, "720p")
	if _, err := s.Submit("720p", 1, "sub1", []Segment{seg(5, shaA)}); err != nil {
		t.Fatal(err)
	}
	revBefore := s.Snapshot().Revision
	// Batch mixes a fresh seq (0) with a conflicting rewrite of seq 5.
	_, err := s.Submit("720p", 1, "sub2", []Segment{seg(0, shaA), seg(5, shaB)})
	if KindOf(err) != KindConflict {
		t.Fatalf("expected conflict, got %v", err)
	}
	snap := s.Snapshot()
	if snap.Revision != revBefore {
		t.Fatalf("failed batch must not bump revision")
	}
	// seq 0 must NOT have been written (all-or-nothing).
	if snap.Renditions["720p"].Head != nil {
		t.Fatalf("partial write detected: head should still be nil")
	}
	if snap.Renditions["720p"].SegmentCount != 1 {
		t.Fatalf("only the original segment should exist, got %d", snap.Renditions["720p"].SegmentCount)
	}
}

func TestInvalidSegmentRejected(t *testing.T) {
	s := mustStream(t, "720p")
	if _, err := s.Submit("720p", 1, "s", []Segment{{Sequence: 0, DurationMs: 0, SHA256: shaA}}); KindOf(err) != KindValidation {
		t.Fatalf("zero duration should be validation error, got %v", err)
	}
	if _, err := s.Submit("720p", 1, "s", []Segment{{Sequence: 0, DurationMs: 1, SHA256: "bad"}}); KindOf(err) != KindValidation {
		t.Fatalf("bad sha should be validation error, got %v", err)
	}
	if _, err := s.Submit("nope", 1, "s", []Segment{seg(0, shaA)}); KindOf(err) != KindValidation {
		t.Fatalf("unknown rendition should be validation error, got %v", err)
	}
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
