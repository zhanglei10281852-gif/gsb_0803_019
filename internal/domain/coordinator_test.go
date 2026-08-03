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

func TestGenerationAdvanceResetsState(t *testing.T) {
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

	res, err := c.SubmitSegments(context.Background(), SubmitSegmentsInput{
		StreamID: "s1", Rendition: "720p", Generation: 3, SubmissionID: "s3",
		Segments: []Segment{seg(0, 4000, "c0")},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res.Generation != 3 {
		t.Fatalf("expected generation 3, got %d", res.Generation)
	}
	if res.HeadSequence != 1 {
		t.Fatalf("head should reset to 1, got %d", res.HeadSequence)
	}
	if res.Watermark.Sequence != 0 || res.Watermark.TimeMs != 0 {
		t.Fatalf("watermark should reset to 0 after generation advance, got %+v", res.Watermark)
	}
	st, _ := c.GetStatus(context.Background(), "s1")
	if st.Generation != 3 {
		t.Fatalf("expected status generation 3, got %d", st.Generation)
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
