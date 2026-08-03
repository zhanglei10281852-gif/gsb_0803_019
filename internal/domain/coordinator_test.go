package domain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
)

type testStore struct {
	mu      sync.RWMutex
	streams map[string]*Stream
}

func newTestStore() *testStore {
	return &testStore{streams: make(map[string]*Stream)}
}

func (t *testStore) CreateStream(ctx context.Context, s *Stream) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.streams[s.ID]; ok {
		return NewError(ErrCodeStreamExists, "exists")
	}
	t.streams[s.ID] = s
	return nil
}

func (t *testStore) GetStream(ctx context.Context, id string) (*Stream, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.streams[id]
	return s, ok
}

func testHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func seg(seq int64, dur int64, content string) Segment {
	return Segment{Sequence: seq, DurationMs: dur, Sha256: testHash(content)}
}

func newTestCoordinator() *Coordinator {
	return NewCoordinator(newTestStore())
}

func createTestStream(t *testing.T, c *Coordinator, id string, renditions []string) {
	t.Helper()
	_, err := c.CreateStream(context.Background(), CreateStreamInput{
		StreamID:         id,
		ActiveRenditions: renditions,
	})
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
}

func TestCreateStream(t *testing.T) {
	c := newTestCoordinator()
	view, err := c.CreateStream(context.Background(), CreateStreamInput{
		StreamID:         "s1",
		ActiveRenditions: []string{"720p", "480p"},
	})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	if view.StreamID != "s1" || view.Revision != 0 {
		t.Fatalf("unexpected view: %+v", view)
	}
	if len(view.ActiveRenditions) != 2 {
		t.Fatalf("expected 2 renditions, got %d", len(view.ActiveRenditions))
	}
	for _, r := range []string{"720p", "480p"} {
		rv, ok := view.Renditions[r]
		if !ok {
			t.Fatalf("rendition %s missing", r)
		}
		if rv.HeadSequence != 0 || rv.ContiguousTimeMs != 0 {
			t.Fatalf("unexpected initial rendition view: %+v", rv)
		}
	}
	if view.Watermark.Sequence != 0 || view.Watermark.TimeMs != 0 {
		t.Fatalf("unexpected initial watermark: %+v", view.Watermark)
	}
}

func TestCreateStreamValidation(t *testing.T) {
	c := newTestCoordinator()
	cases := []struct {
		name string
		in   CreateStreamInput
	}{
		{"empty id", CreateStreamInput{ActiveRenditions: []string{"720p"}}},
		{"no renditions", CreateStreamInput{StreamID: "s1"}},
		{"empty rendition name", CreateStreamInput{StreamID: "s1", ActiveRenditions: []string{""}}},
		{"duplicate rendition", CreateStreamInput{StreamID: "s1", ActiveRenditions: []string{"a", "a"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := c.CreateStream(context.Background(), tc.in); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestCreateDuplicateStream(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	_, err := c.CreateStream(context.Background(), CreateStreamInput{
		StreamID:         "s1",
		ActiveRenditions: []string{"480p"},
	})
	if err == nil {
		t.Fatal("expected duplicate error")
	}
	de, ok := err.(*Error)
	if !ok || de.Code != ErrCodeStreamExists {
		t.Fatalf("expected STREAM_ALREADY_EXISTS, got %v", err)
	}
}

func TestSubmitBasicContiguousAndWatermark(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p", "480p"})

	res, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "sub-a",
		Segments: []Segment{seg(0, 2000, "a0"), seg(1, 2000, "a1")},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res.Revision != 1 || res.HeadSequence != 2 || res.Accepted != 2 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if res.Watermark.Sequence != 0 || res.Watermark.TimeMs != 0 {
		t.Fatalf("watermark should be 0 when other rendition empty: %+v", res.Watermark)
	}

	res2, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "480p", Generation: 1, SubmissionID: "sub-b",
		Segments: []Segment{seg(0, 2000, "b0")},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res2.Watermark.Sequence != 1 || res2.Watermark.TimeMs != 2000 {
		t.Fatalf("watermark should be seq=1 time=2000, got %+v", res2.Watermark)
	}
	if res2.Revision != 2 {
		t.Fatalf("expected revision 2, got %d", res2.Revision)
	}

	st, err := c.GetStatus(context.Background(), "s1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Revision != 2 {
		t.Fatalf("expected status revision 2, got %d", st.Revision)
	}
	if st.Renditions["720p"].HeadSequence != 2 || st.Renditions["480p"].HeadSequence != 1 {
		t.Fatalf("unexpected heads: %+v", st.Renditions)
	}
}

func TestOutOfOrderBufferedHeadDoesNotCrossGap(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})

	res, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "sub-1",
		Segments: []Segment{seg(0, 2000, "a0"), seg(2, 2000, "a2")},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res.HeadSequence != 1 {
		t.Fatalf("head should stop at 1 due to gap at 2? got %d", res.HeadSequence)
	}
	if res.Watermark.Sequence != 1 {
		t.Fatalf("watermark should be 1, got %d", res.Watermark.Sequence)
	}
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Renditions["720p"].BufferedSegments != 1 {
		t.Fatalf("expected 1 buffered, got %d", st.Renditions["720p"].BufferedSegments)
	}

	fill, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "sub-2",
		Segments: []Segment{seg(1, 2000, "a1")},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if fill.HeadSequence != 3 {
		t.Fatalf("head should advance to 3 after filling gap, got %d", fill.HeadSequence)
	}
	if fill.Watermark.Sequence != 3 || fill.Watermark.TimeMs != 6000 {
		t.Fatalf("unexpected watermark after fill: %+v", fill.Watermark)
	}
	st, _ = c.GetStatus(context.Background(), "s1")
	if st.Renditions["720p"].BufferedSegments != 0 {
		t.Fatalf("expected 0 buffered after drain, got %d", st.Renditions["720p"].BufferedSegments)
	}
}

func TestIdempotentRetryDoesNotAdvanceRevision(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	segs := []Segment{seg(0, 2000, "a0"), seg(1, 2000, "a1")}
	in := SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "sub-1", Segments: segs,
	}
	first, err := c.SubmitSegments(context.Background(), in)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if first.Revision != 1 || first.Idempotent {
		t.Fatalf("unexpected first result: %+v", first)
	}

	retry, err := c.SubmitSegments(context.Background(), in)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !retry.Idempotent {
		t.Fatal("expected idempotent=true on retry")
	}
	if retry.Revision != 1 {
		t.Fatalf("retry must not advance revision; got %d", retry.Revision)
	}
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Revision != 1 {
		t.Fatalf("status revision should still be 1, got %d", st.Revision)
	}
}

func TestIdempotentRetrySha256CaseInsensitive(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	lower := testHash("a0")
	upper := ""
	for _, ch := range lower {
		if ch >= 'a' && ch <= 'f' {
			upper += string(ch - ('a' - 'A'))
		} else {
			upper += string(ch)
		}
	}
	in1 := SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "sub-1",
		Segments: []Segment{{Sequence: 0, DurationMs: 2000, Sha256: lower}},
	}
	if _, err := c.SubmitSegments(context.Background(), in1); err != nil {
		t.Fatalf("submit: %v", err)
	}
	in2 := in1
	in2.Segments = []Segment{{Sequence: 0, DurationMs: 2000, Sha256: upper}}
	res, err := c.SubmitSegments(context.Background(), in2)
	if err != nil {
		t.Fatalf("retry with different sha256 casing should be idempotent: %v", err)
	}
	if !res.Idempotent || res.Revision != 1 {
		t.Fatalf("expected idempotent at revision 1, got %+v", res)
	}
}

func TestSameSubmissionIDDifferentContentConflict(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	in1 := SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "sub-1",
		Segments: []Segment{seg(0, 2000, "a0")},
	}
	if _, err := c.SubmitSegments(context.Background(), in1); err != nil {
		t.Fatalf("submit: %v", err)
	}
	in2 := in1
	in2.Segments = []Segment{seg(0, 2000, "different")}
	_, err := c.SubmitSegments(context.Background(), in2)
	if err == nil {
		t.Fatal("expected conflict")
	}
	de, ok := err.(*Error)
	if !ok || de.Code != ErrCodeConflict {
		t.Fatalf("expected CONFLICT, got %v", err)
	}
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Revision != 1 {
		t.Fatalf("revision must not change on conflict; got %d", st.Revision)
	}
}

func TestSegmentContentConflictNoPartial(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	_, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "sub-1",
		Segments: []Segment{seg(0, 2000, "original")},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	_, err = c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "sub-2",
		Segments: []Segment{
			seg(1, 2000, "new-1"),
			seg(0, 2000, "tampered"),
		},
	})
	if err == nil {
		t.Fatal("expected conflict for tampered segment 0")
	}
	de, ok := err.(*Error)
	if !ok || de.Code != ErrCodeConflict {
		t.Fatalf("expected CONFLICT, got %v", err)
	}
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Renditions["720p"].HeadSequence != 1 {
		t.Fatalf("head should remain 1, got %d", st.Renditions["720p"].HeadSequence)
	}
	if st.Renditions["720p"].BufferedSegments != 0 {
		t.Fatalf("no partial segments should remain; buffered=%d", st.Renditions["720p"].BufferedSegments)
	}
}

func TestValidationFailureNoPartial(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})

	cases := []struct {
		name string
		in   SubmitSegmentsInput
	}{
		{"bad sha256", SubmitSegmentsInput{
			StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "s",
			Segments: []Segment{{Sequence: 0, DurationMs: 2000, Sha256: "zzz"}},
		}},
		{"bad duration", SubmitSegmentsInput{
			StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "s",
			Segments: []Segment{seg(0, 0, "a")},
		}},
		{"negative sequence", SubmitSegmentsInput{
			StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "s",
			Segments: []Segment{seg(-1, 2000, "a")},
		}},
		{"duplicate in batch", SubmitSegmentsInput{
			StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "s",
			Segments: []Segment{seg(0, 2000, "a"), seg(0, 2000, "a")},
		}},
		{"inactive rendition", SubmitSegmentsInput{
			StreamID: "s1", Rendition: "audio", Generation: 1, SubmissionID: "s",
			Segments: []Segment{seg(0, 2000, "a")},
		}},
		{"empty segments", SubmitSegmentsInput{
			StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "s",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.SubmitSegments(context.Background(), tc.in)
			if err == nil {
				t.Fatal("expected error")
			}
			st, _ := c.GetStatus(context.Background(), "s1")
			if st.Revision != 0 {
				t.Fatalf("revision must stay 0 after validation failure, got %d", st.Revision)
			}
			if st.Renditions["720p"].HeadSequence != 0 || st.Renditions["720p"].BufferedSegments != 0 {
				t.Fatalf("state should be untouched: %+v", st.Renditions["720p"])
			}
		})
	}
}

func TestStaleGenerationRejected(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	_, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 2, SubmissionID: "s1",
		Segments: []Segment{seg(0, 2000, "a")},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	_, err = c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "s2",
		Segments: []Segment{seg(0, 2000, "b")},
	})
	if err == nil {
		t.Fatal("expected stale generation error")
	}
	de, ok := err.(*Error)
	if !ok || de.Code != ErrCodeStaleGeneration {
		t.Fatalf("expected STALE_GENERATION, got %v", err)
	}
}

func TestCutoverResetsStateAndFencesOldGeneration(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p", "480p"})
	_, _ = c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "s1",
		Segments: []Segment{seg(0, 2000, "a0"), seg(1, 2000, "a1")},
	})
	_, _ = c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "480p", Generation: 1, SubmissionID: "s2",
		Segments: []Segment{seg(0, 2000, "b0")},
	})

	res, err := c.Cutover(context.Background(), CutoverInput{
		StreamID: "s1", Generation: 3, CutoverID: "co-1", ExpectedRevision: 2,
		Renditions: map[string]int64{"720p": 2, "480p": 1},
	})
	if err != nil {
		t.Fatalf("cutover: %v", err)
	}
	if res.Generation != 3 || res.Revision != 3 {
		t.Fatalf("unexpected cutover result: %+v", res)
	}
	if res.Idempotent {
		t.Fatal("fresh cutover must not be idempotent")
	}
	if res.Renditions["720p"].StartSequence != 2 || res.Renditions["720p"].HeadSequence != 2 {
		t.Fatalf("720p should start at 2: %+v", res.Renditions["720p"])
	}
	if res.Renditions["480p"].StartSequence != 1 || res.Renditions["480p"].HeadSequence != 1 {
		t.Fatalf("480p should start at 1: %+v", res.Renditions["480p"])
	}
	if res.Watermark.Sequence != 2 || res.Watermark.TimeMs != 0 {
		t.Fatalf("watermark should be at max start (2) with 0 new time: %+v", res.Watermark)
	}

	oldSubmit, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "late",
		Segments: []Segment{seg(2, 2000, "late-a2")},
	})
	if err == nil {
		t.Fatalf("expected fencing error, got result: %+v", oldSubmit)
	}
	de, ok := err.(*Error)
	if !ok || de.Code != ErrCodeStaleGeneration {
		t.Fatalf("expected STALE_GENERATION, got %v", err)
	}

	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Revision != 3 {
		t.Fatalf("fenced late write must not change revision; got %d", st.Revision)
	}
	if st.Renditions["720p"].HeadSequence != 2 || st.Renditions["480p"].HeadSequence != 1 {
		t.Fatalf("fenced late write must not move heads: %+v", st.Renditions)
	}
	if st.Watermark.Sequence != 2 {
		t.Fatalf("watermark must not move on fenced write: %+v", st.Watermark)
	}

	newRes, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 3, SubmissionID: "s3",
		Segments: []Segment{seg(2, 2000, "c2")},
	})
	if err != nil {
		t.Fatalf("new generation submit: %v", err)
	}
	if newRes.HeadSequence != 3 {
		t.Fatalf("720p head should advance to 3, got %d", newRes.HeadSequence)
	}
	if newRes.Watermark.Sequence != 2 || newRes.Watermark.TimeMs != 0 {
		t.Fatalf("watermark must wait for 480p to reach 2: %+v", newRes.Watermark)
	}
}

func TestCutoverIdempotentRetryDoesNotSwitchAgain(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	in := CutoverInput{
		StreamID: "s1", Generation: 2, CutoverID: "co-1", ExpectedRevision: 0,
		Renditions: map[string]int64{"720p": 5},
	}
	first, err := c.Cutover(context.Background(), in)
	if err != nil {
		t.Fatalf("cutover: %v", err)
	}
	if first.Revision != 1 || first.Idempotent {
		t.Fatalf("unexpected first: %+v", first)
	}

	_, _ = c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 2, SubmissionID: "s1",
		Segments: []Segment{seg(5, 2000, "c5")},
	})

	retry, err := c.Cutover(context.Background(), in)
	if err != nil {
		t.Fatalf("retry cutover: %v", err)
	}
	if !retry.Idempotent {
		t.Fatal("retry must be idempotent")
	}
	if retry.Revision != 1 {
		t.Fatalf("idempotent cutover must report original revision 1, got %d", retry.Revision)
	}
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Revision != 2 {
		t.Fatalf("stream revision should be 2 after the segment submit, got %d", st.Revision)
	}
	if st.Renditions["720p"].HeadSequence != 6 {
		t.Fatalf("head should have advanced from the new-gen submit, got %d", st.Renditions["720p"].HeadSequence)
	}
}

func TestCutoverSameIDDifferentContentConflict(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	_, err := c.Cutover(context.Background(), CutoverInput{
		StreamID: "s1", Generation: 2, CutoverID: "co-1", ExpectedRevision: 0,
		Renditions: map[string]int64{"720p": 5},
	})
	if err != nil {
		t.Fatalf("cutover: %v", err)
	}

	cases := []struct {
		name string
		in   CutoverInput
	}{
		{"different generation", CutoverInput{
			StreamID: "s1", Generation: 3, CutoverID: "co-1", ExpectedRevision: 1,
			Renditions: map[string]int64{"720p": 5},
		}},
		{"different start", CutoverInput{
			StreamID: "s1", Generation: 2, CutoverID: "co-1", ExpectedRevision: 1,
			Renditions: map[string]int64{"720p": 6},
		}},
		{"different expected revision", CutoverInput{
			StreamID: "s1", Generation: 2, CutoverID: "co-1", ExpectedRevision: 99,
			Renditions: map[string]int64{"720p": 5},
		}},
		{"different rendition set", CutoverInput{
			StreamID: "s1", Generation: 2, CutoverID: "co-1", ExpectedRevision: 1,
			Renditions: map[string]int64{"720p": 5, "480p": 5},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.Cutover(context.Background(), tc.in)
			if err == nil {
				t.Fatal("expected conflict")
			}
			de, ok := err.(*Error)
			if !ok || de.Code != ErrCodeCutoverConflict {
				t.Fatalf("expected CUTOVER_CONFLICT, got %v", err)
			}
		})
	}
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Generation != 2 || st.Revision != 1 {
		t.Fatalf("state must not change on drift conflict: gen=%d rev=%d", st.Generation, st.Revision)
	}
}

func TestCutoverExpectedRevisionMismatch(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	_, _ = c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "s1",
		Segments: []Segment{seg(0, 2000, "a0")},
	})
	_, err := c.Cutover(context.Background(), CutoverInput{
		StreamID: "s1", Generation: 2, CutoverID: "co-1", ExpectedRevision: 0,
		Renditions: map[string]int64{"720p": 1},
	})
	if err == nil {
		t.Fatal("expected revision mismatch")
	}
	de, ok := err.(*Error)
	if !ok || de.Code != ErrCodeRevisionMismatch {
		t.Fatalf("expected REVISION_MISMATCH, got %v", err)
	}
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Generation != 1 || st.Revision != 1 {
		t.Fatalf("failed cutover must not change state: gen=%d rev=%d", st.Generation, st.Revision)
	}
}

func TestCutoverGenerationRegression(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	_, _ = c.Cutover(context.Background(), CutoverInput{
		StreamID: "s1", Generation: 5, CutoverID: "co-1", ExpectedRevision: 0,
		Renditions: map[string]int64{"720p": 0},
	})
	_, err := c.Cutover(context.Background(), CutoverInput{
		StreamID: "s1", Generation: 5, CutoverID: "co-2", ExpectedRevision: 1,
		Renditions: map[string]int64{"720p": 0},
	})
	if err == nil {
		t.Fatal("expected stale generation for equal/regressed generation")
	}
	de, ok := err.(*Error)
	if !ok || de.Code != ErrCodeStaleGeneration {
		t.Fatalf("expected STALE_GENERATION, got %v", err)
	}
	_, err = c.Cutover(context.Background(), CutoverInput{
		StreamID: "s1", Generation: 4, CutoverID: "co-3", ExpectedRevision: 1,
		Renditions: map[string]int64{"720p": 0},
	})
	if err == nil {
		t.Fatal("expected stale generation for lower generation")
	}
}

func TestCutoverInvalidRenditionSet(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	cases := []struct {
		name string
		in   CutoverInput
	}{
		{"empty renditions", CutoverInput{
			StreamID: "s1", Generation: 2, CutoverID: "co", ExpectedRevision: 0,
		}},
		{"empty name", CutoverInput{
			StreamID: "s1", Generation: 2, CutoverID: "co", ExpectedRevision: 0,
			Renditions: map[string]int64{"": 0},
		}},
		{"negative start", CutoverInput{
			StreamID: "s1", Generation: 2, CutoverID: "co", ExpectedRevision: 0,
			Renditions: map[string]int64{"720p": -1},
		}},
		{"missing cutover id", CutoverInput{
			StreamID: "s1", Generation: 2, ExpectedRevision: 0,
			Renditions: map[string]int64{"720p": 0},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.Cutover(context.Background(), tc.in)
			if err == nil {
				t.Fatal("expected error")
			}
			de, ok := err.(*Error)
			if !ok || de.Code != ErrCodeInvalidRequest {
				t.Fatalf("expected INVALID_REQUEST, got %v", err)
			}
		})
	}
}

func TestCutoverCanChangeActiveRenditionSet(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p", "480p"})
	res, err := c.Cutover(context.Background(), CutoverInput{
		StreamID: "s1", Generation: 2, CutoverID: "co-1", ExpectedRevision: 0,
		Renditions: map[string]int64{"720p": 0, "1080p": 0},
	})
	if err != nil {
		t.Fatalf("cutover: %v", err)
	}
	if _, ok := res.Renditions["480p"]; ok {
		t.Fatal("480p should have been removed during cutover")
	}
	if _, ok := res.Renditions["1080p"]; !ok {
		t.Fatal("1080p should have been added during cutover")
	}
	if len(res.ActiveRenditions) != 2 {
		t.Fatalf("expected 2 active renditions, got %v", res.ActiveRenditions)
	}
	_, err = c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "480p", Generation: 2, SubmissionID: "s1",
		Segments: []Segment{seg(0, 2000, "b0")},
	})
	if err == nil {
		t.Fatal("removed rendition must not accept segments")
	}
}

func TestPartialNewPrimaryReadinessWatermarkWaits(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p", "480p"})
	_, err := c.Cutover(context.Background(), CutoverInput{
		StreamID: "s1", Generation: 2, CutoverID: "co-1", ExpectedRevision: 0,
		Renditions: map[string]int64{"720p": 10, "480p": 8},
	})
	if err != nil {
		t.Fatalf("cutover: %v", err)
	}

	r720, _ := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 2, SubmissionID: "s1",
		Segments: []Segment{seg(10, 2000, "c10"), seg(11, 2000, "c11")},
	})
	if r720.HeadSequence != 12 {
		t.Fatalf("720p head should be 12, got %d", r720.HeadSequence)
	}
	if r720.Watermark.Sequence != 10 || r720.Watermark.TimeMs != 0 {
		t.Fatalf("watermark must hold at max start 10 until 480p catches up: %+v", r720.Watermark)
	}

	r480, _ := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "480p", Generation: 2, SubmissionID: "s2",
		Segments: []Segment{seg(8, 2000, "d8"), seg(9, 2000, "d9")},
	})
	if r480.HeadSequence != 10 {
		t.Fatalf("480p head should be 10, got %d", r480.HeadSequence)
	}
	if r480.Watermark.Sequence != 10 || r480.Watermark.TimeMs != 4000 {
		t.Fatalf("watermark should advance to 10 with 4000ms once both reach common floor: %+v", r480.Watermark)
	}

	_, _ = c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "480p", Generation: 2, SubmissionID: "s3",
		Segments: []Segment{seg(10, 2000, "d10")},
	})
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Watermark.Sequence != 11 {
		t.Fatalf("watermark should be 11 (min head) after 480p catches up, got %d", st.Watermark.Sequence)
	}
}

func TestSubmitHigherGenerationWithoutCutoverRejected(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	_, _ = c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "s1",
		Segments: []Segment{seg(0, 2000, "a0")},
	})
	_, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 2, SubmissionID: "s2",
		Segments: []Segment{seg(1, 2000, "a1")},
	})
	if err == nil {
		t.Fatal("expected cutover-required error")
	}
	de, ok := err.(*Error)
	if !ok || de.Code != ErrCodeCutoverRequired {
		t.Fatalf("expected CUTOVER_REQUIRED, got %v", err)
	}
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Generation != 1 || st.Revision != 1 {
		t.Fatalf("rejected submit must not change state: gen=%d rev=%d", st.Generation, st.Revision)
	}
}

func TestSegmentBeforeStartSequenceRejected(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	_, _ = c.Cutover(context.Background(), CutoverInput{
		StreamID: "s1", Generation: 2, CutoverID: "co-1", ExpectedRevision: 0,
		Renditions: map[string]int64{"720p": 5},
	})
	_, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 2, SubmissionID: "s1",
		Segments: []Segment{seg(4, 2000, "below")},
	})
	if err == nil {
		t.Fatal("expected out-of-range error")
	}
	de, ok := err.(*Error)
	if !ok || de.Code != ErrCodeSegmentOutOfRange {
		t.Fatalf("expected SEGMENT_OUT_OF_RANGE, got %v", err)
	}
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Renditions["720p"].HeadSequence != 5 || st.Renditions["720p"].BufferedSegments != 0 {
		t.Fatalf("rejected batch must leave no partial state: %+v", st.Renditions["720p"])
	}
}

func TestConcurrentCutoversOneWins(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	revMismatch := 0
	success := 0
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := c.Cutover(context.Background(), CutoverInput{
				StreamID: "s1", Generation: int64(i + 2), CutoverID: fmt.Sprintf("co-%d", i),
				ExpectedRevision: 0,
				Renditions:       map[string]int64{"720p": int64(i)},
			})
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			success++
			_ = res
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		de, ok := err.(*Error)
		if !ok || de.Code != ErrCodeRevisionMismatch {
			t.Fatalf("unexpected error: %v", err)
		}
		revMismatch++
	}
	if success != 1 {
		t.Fatalf("exactly one cutover should succeed, got %d (mismatches=%d)", success, revMismatch)
	}
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Revision != 1 {
		t.Fatalf("revision should be exactly 1, got %d", st.Revision)
	}
}

func TestInFlightOldWriteLinearizedBeforeOrAfterCutover(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	_, _ = c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "old-1",
		Segments: []Segment{seg(0, 2000, "a0")},
	})

	var wg sync.WaitGroup
	wg.Add(2)
	oldDone := make(chan error, 1)
	cutDone := make(chan error, 1)
	go func() {
		defer wg.Done()
		_, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
			StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "old-2",
			Segments: []Segment{seg(1, 2000, "a1")},
		})
		oldDone <- err
	}()
	go func() {
		defer wg.Done()
		_, err := c.Cutover(context.Background(), CutoverInput{
			StreamID: "s1", Generation: 2, CutoverID: "co-1", ExpectedRevision: 1,
			Renditions: map[string]int64{"720p": 2},
		})
		cutDone <- err
	}()
	wg.Wait()

	oldErr := <-oldDone
	cutErr := <-cutDone
	st, _ := c.GetStatus(context.Background(), "s1")

	if cutErr == nil {
		if oldErr == nil {
			t.Fatal("if cutover landed first, the old write must be fenced")
		}
		if de, ok := oldErr.(*Error); !ok || de.Code != ErrCodeStaleGeneration {
			t.Fatalf("expected STALE_GENERATION for fenced old write, got %v", oldErr)
		}
		if st.Generation != 2 || st.Revision != 2 {
			t.Fatalf("expected gen=2 rev=2 after cutover+fence, got gen=%d rev=%d", st.Generation, st.Revision)
		}
		if st.Renditions["720p"].HeadSequence != 2 {
			t.Fatalf("head should be cutover start 2, got %d", st.Renditions["720p"].HeadSequence)
		}
	} else {
		if de, ok := cutErr.(*Error); !ok || de.Code != ErrCodeRevisionMismatch {
			t.Fatalf("if old write landed first, cutover should fail REVISION_MISMATCH, got %v", cutErr)
		}
		if oldErr != nil {
			t.Fatalf("old write before cutover should succeed, got %v", oldErr)
		}
		if st.Generation != 1 || st.Revision != 2 {
			t.Fatalf("expected gen=1 rev=2 when old write landed first, got gen=%d rev=%d", st.Generation, st.Revision)
		}
		if st.Renditions["720p"].HeadSequence != 2 {
			t.Fatalf("head should be 2 after old write, got %d", st.Renditions["720p"].HeadSequence)
		}
	}
}

func TestCutoverToMissingStream(t *testing.T) {
	c := newTestCoordinator()
	_, err := c.Cutover(context.Background(), CutoverInput{
		StreamID: "nope", Generation: 1, CutoverID: "co", ExpectedRevision: 0,
		Renditions: map[string]int64{"720p": 0},
	})
	if err == nil {
		t.Fatal("expected not found")
	}
	de, ok := err.(*Error)
	if !ok || de.Code != ErrCodeStreamNotFound {
		t.Fatalf("expected STREAM_NOT_FOUND, got %v", err)
	}
}

func TestRedeliverAlreadyAppliedSegmentMatchingContent(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	original := seg(0, 2000, "a0")
	_, _ = c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "s1",
		Segments: []Segment{original},
	})
	res, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 1, SubmissionID: "s2",
		Segments: []Segment{original},
	})
	if err != nil {
		t.Fatalf("redeliver with matching content should succeed: %v", err)
	}
	if res.Accepted != 0 {
		t.Fatalf("expected 0 newly accepted, got %d", res.Accepted)
	}
	if res.HeadSequence != 1 {
		t.Fatalf("head should remain 1, got %d", res.HeadSequence)
	}
}

func TestWatermarkIsMinAcrossActiveRenditions(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p", "480p", "360p"})
	submit := func(rendition string, sub string, seqs ...int64) {
		t.Helper()
		segs := make([]Segment, 0, len(seqs))
		for _, q := range seqs {
			segs = append(segs, seg(q, 1000, fmt.Sprintf("%s-%d", rendition, q)))
		}
		_, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
			StreamID: "s1", Rendition: rendition, Generation: 1, SubmissionID: sub,
			Segments: segs,
		})
		if err != nil {
			t.Fatalf("submit %s: %v", rendition, err)
		}
	}
	submit("720p", "a", 0, 1, 2, 3)
	submit("480p", "b", 0, 1)
	submit("360p", "c", 0, 1, 2)

	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Watermark.Sequence != 2 {
		t.Fatalf("expected watermark seq=2 (min head), got %d", st.Watermark.Sequence)
	}
	if st.Watermark.TimeMs != 2000 {
		t.Fatalf("expected watermark time=2000, got %d", st.Watermark.TimeMs)
	}
}

func TestConcurrentSubmissionsSerializable(t *testing.T) {
	c := newTestCoordinator()
	createTestStream(t, c, "s1", []string{"720p"})
	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seq := int64(i)
			_, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
				StreamID: "s1", Rendition: "720p", Generation: 1,
				SubmissionID: fmt.Sprintf("sub-%d", i),
				Segments:     []Segment{seg(seq, 1000, fmt.Sprintf("c-%d", i))},
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent submit error: %v", err)
	}

	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Renditions["720p"].HeadSequence != n {
		t.Fatalf("expected head %d, got %d (gaps present)", n, st.Renditions["720p"].HeadSequence)
	}
	if st.Revision != n {
		t.Fatalf("expected revision %d, got %d", n, st.Revision)
	}
	if st.Watermark.Sequence != n {
		t.Fatalf("expected watermark %d, got %d", n, st.Watermark.Sequence)
	}
}

func TestGetStatusNotFound(t *testing.T) {
	c := newTestCoordinator()
	_, err := c.GetStatus(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected not found")
	}
	de, ok := err.(*Error)
	if !ok || de.Code != ErrCodeStreamNotFound {
		t.Fatalf("expected STREAM_NOT_FOUND, got %v", err)
	}
}

func TestHealth(t *testing.T) {
	c := newTestCoordinator()
	h := c.Health(context.Background())
	if h.Status != "ok" {
		t.Fatalf("unexpected health: %+v", h)
	}
}
