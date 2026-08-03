package coord_test

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	"livecoord/internal/coord"
)

func newCoordinator() *coord.Coordinator {
	return coord.NewCoordinator(coord.NewStore())
}

func mustCreate(t *testing.T, c *coord.Coordinator, id string, rends ...string) coord.Status {
	t.Helper()
	st, err := c.CreateStream(id, rends)
	if err != nil {
		t.Fatalf("CreateStream(%q): %v", id, err)
	}
	return st
}

func mustIngest(t *testing.T, c *coord.Coordinator, stream string, req coord.IngestRequest) coord.IngestResult {
	t.Helper()
	res, err := c.Ingest(stream, req)
	if err != nil {
		t.Fatalf("Ingest(%s/%s gen=%d sub=%s): %v", stream, req.Rendition, req.Generation, req.SubmissionID, err)
	}
	return res
}

func seg(seq int64) coord.Segment {
	return coord.Segment{Sequence: seq, DurationMs: 2000, SHA256: fmt.Sprintf("%064x", seq+1)}
}

func batch(subID, rendition string, gen int64, seqs ...int64) coord.IngestRequest {
	segs := make([]coord.Segment, len(seqs))
	for i, s := range seqs {
		segs[i] = seg(s)
	}
	return coord.IngestRequest{
		SubmissionID: subID,
		Rendition:    rendition,
		Generation:   gen,
		Segments:     segs,
	}
}

func codeOf(t *testing.T, err error) string {
	t.Helper()
	var ce *coord.Error
	if err == nil || !errors.As(err, &ce) {
		t.Fatalf("expected *coord.Error, got %v", err)
	}
	return ce.Code
}

func TestContiguousHeadNeverCrossesGaps(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a", "b")

	res := mustIngest(t, c, "s1", batch("sub-1", "a", 0, 1, 2))
	if res.Head != -1 {
		t.Fatalf("head = %d, want -1 (sequence 0 missing)", res.Head)
	}
	if res.Watermark != -1 {
		t.Fatalf("watermark = %d, want -1", res.Watermark)
	}

	res = mustIngest(t, c, "s1", batch("sub-2", "a", 0, 0))
	if res.Head != 2 {
		t.Fatalf("head = %d, want 2 after filling the gap", res.Head)
	}

	res = mustIngest(t, c, "s1", batch("sub-3", "b", 0, 0, 1))
	if res.Watermark != 1 {
		t.Fatalf("watermark = %d, want 1 (min of heads 2 and 1)", res.Watermark)
	}

	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Renditions["a"].Head != 2 || st.Renditions["a"].Buffered != 0 {
		t.Fatalf("rendition a = %+v, want head 2 buffered 0", st.Renditions["a"])
	}
	if st.Watermark != 1 {
		t.Fatalf("status watermark = %d, want 1", st.Watermark)
	}
}

func TestOutOfOrderWithinSingleBatch(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	res := mustIngest(t, c, "s1", batch("sub-1", "a", 0, 2, 0, 1))
	if res.Head != 2 {
		t.Fatalf("head = %d, want 2", res.Head)
	}
	if res.Applied != 3 {
		t.Fatalf("applied = %d, want 3", res.Applied)
	}
}

func TestRevisionMonotonic(t *testing.T) {
	c := newCoordinator()
	st := mustCreate(t, c, "s1", "a")
	if st.Revision != 1 {
		t.Fatalf("create revision = %d, want 1", st.Revision)
	}
	r1 := mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0))
	r2 := mustIngest(t, c, "s1", batch("sub-2", "a", 0, 1))
	if r1.Revision != 2 || r2.Revision != 3 {
		t.Fatalf("revisions = %d, %d, want 2, 3", r1.Revision, r2.Revision)
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 3 {
		t.Fatalf("status revision = %d, want 3", st.Revision)
	}
}

func TestGenerationAdvanceResetsState(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a", "b")
	mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0, 1))
	mustIngest(t, c, "s1", batch("sub-2", "b", 0, 0, 1))

	res := mustIngest(t, c, "s1", batch("sub-3", "a", 1, 5))
	if res.Generation != 1 {
		t.Fatalf("generation = %d, want 1", res.Generation)
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Generation != 1 {
		t.Fatalf("status generation = %d, want 1", st.Generation)
	}
	if st.Watermark != -1 {
		t.Fatalf("watermark = %d, want -1 after generation advance", st.Watermark)
	}
	if st.Renditions["a"].Head != -1 || st.Renditions["a"].Buffered != 1 {
		t.Fatalf("rendition a = %+v, want head -1 buffered 1", st.Renditions["a"])
	}
	if st.Renditions["b"].Head != -1 || st.Renditions["b"].Buffered != 0 {
		t.Fatalf("rendition b = %+v, want head -1 buffered 0 (reset)", st.Renditions["b"])
	}

	mustIngest(t, c, "s1", batch("sub-4", "a", 1, 0))
	res = mustIngest(t, c, "s1", batch("sub-5", "b", 1, 0))
	if res.Watermark != 0 {
		t.Fatalf("watermark = %d, want 0", res.Watermark)
	}
}

func TestStaleGenerationRejected(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	mustIngest(t, c, "s1", batch("sub-1", "a", 1, 0))

	_, err := c.Ingest("s1", batch("sub-2", "a", 0, 0))
	if got := codeOf(t, err); got != coord.CodeStaleGeneration {
		t.Fatalf("code = %q, want %q", got, coord.CodeStaleGeneration)
	}
}

func TestReplayAcrossGenerationAdvance(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	orig := mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0))
	mustIngest(t, c, "s1", batch("sub-2", "a", 1, 0))

	replayed, err := c.Ingest("s1", batch("sub-1", "a", 0, 0))
	if err != nil {
		t.Fatalf("replay of committed submission must succeed, got %v", err)
	}
	if !replayed.Replayed || replayed.Revision != orig.Revision {
		t.Fatalf("replay = %+v, want revision %d with replayed=true", replayed, orig.Revision)
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 3 {
		t.Fatalf("status revision = %d, want 3 (replay must not advance)", st.Revision)
	}
}

func TestIdempotentReplay(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	res1 := mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0, 1, 2))
	if res1.Replayed {
		t.Fatal("first commit must not be marked replayed")
	}
	res2 := mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0, 1, 2))
	if !res2.Replayed {
		t.Fatal("retry must be marked replayed")
	}
	if res2.Revision != res1.Revision || res2.Head != res1.Head || res2.Applied != res1.Applied {
		t.Fatalf("replay = %+v, want same committed result as %+v", res2, res1)
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != res1.Revision {
		t.Fatalf("status revision = %d, want %d (replay must not advance)", st.Revision, res1.Revision)
	}
}

func TestIdempotentReplayReorderedSegments(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0, 1, 2))
	res := mustIngest(t, c, "s1", batch("sub-1", "a", 0, 2, 0, 1))
	if !res.Replayed {
		t.Fatal("same content in different order must still be an idempotent replay")
	}
}

func TestSubmissionConflict(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0, 1))

	_, err := c.Ingest("s1", batch("sub-1", "a", 0, 0, 2))
	if got := codeOf(t, err); got != coord.CodeSubmissionConflict {
		t.Fatalf("code = %q, want %q", got, coord.CodeSubmissionConflict)
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 2 {
		t.Fatalf("status revision = %d, want 2 (conflict must not advance)", st.Revision)
	}
}

func TestBatchAtomicityOnValidationFailure(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")

	req := batch("sub-9", "a", 0, 0, 1, 2)
	req.Segments[2].SHA256 = "not-hex"
	_, err := c.Ingest("s1", req)
	if got := codeOf(t, err); got != coord.CodeInvalidArgument {
		t.Fatalf("code = %q, want %q", got, coord.CodeInvalidArgument)
	}

	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 1 || st.Renditions["a"].Head != -1 || st.Renditions["a"].Buffered != 0 {
		t.Fatalf("status = %+v, want untouched stream (revision 1, no segments)", st)
	}

	// A failed validation records nothing: the same submissionId may be
	// reused with corrected content.
	if _, err := c.Ingest("s1", batch("sub-9", "a", 0, 0, 1, 2)); err != nil {
		t.Fatalf("corrected retry with same submissionId: %v", err)
	}
}

func TestBatchAtomicityOnSegmentConflict(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0))

	conflicting := coord.IngestRequest{
		SubmissionID: "sub-2",
		Rendition:    "a",
		Generation:   0,
		Segments: []coord.Segment{
			{Sequence: 0, DurationMs: 2000, SHA256: fmt.Sprintf("%064x", 999)},
			seg(1),
		},
	}
	_, err := c.Ingest("s1", conflicting)
	if got := codeOf(t, err); got != coord.CodeSegmentConflict {
		t.Fatalf("code = %q, want %q", got, coord.CodeSegmentConflict)
	}

	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 2 || st.Renditions["a"].Head != 0 || st.Renditions["a"].Buffered != 0 {
		t.Fatalf("status = %+v, want no partial application (revision 2, head 0, buffered 0)", st)
	}
}

func TestCreateStreamValidation(t *testing.T) {
	c := newCoordinator()
	if _, err := c.CreateStream("s1", []string{"a", "a"}); codeOf(t, err) != coord.CodeInvalidArgument {
		t.Fatalf("duplicate renditions: %v", err)
	}
	if _, err := c.CreateStream("s1", nil); codeOf(t, err) != coord.CodeInvalidArgument {
		t.Fatalf("empty renditions: %v", err)
	}
	mustCreate(t, c, "s1", "a")
	if _, err := c.CreateStream("s1", []string{"a"}); codeOf(t, err) != coord.CodeAlreadyExists {
		t.Fatalf("duplicate stream: %v", err)
	}
}

func TestUnknownStreamAndRendition(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")

	if _, err := c.Status("ghost"); codeOf(t, err) != coord.CodeNotFound {
		t.Fatalf("status of unknown stream: %v", err)
	}
	if _, err := c.Ingest("ghost", batch("sub-1", "a", 0, 0)); codeOf(t, err) != coord.CodeNotFound {
		t.Fatalf("ingest to unknown stream: %v", err)
	}
	if _, err := c.Ingest("s1", batch("sub-1", "ghost", 0, 0)); codeOf(t, err) != coord.CodeInvalidArgument {
		t.Fatalf("ingest to inactive rendition: %v", err)
	}
}

func TestConcurrentDistinctSubmissionsAreLinearizable(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")

	const n = 64
	revs := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := c.Ingest("s1", batch(fmt.Sprintf("sub-%d", i), "a", 0, int64(i)))
			if err != nil {
				t.Errorf("Ingest sub-%d: %v", i, err)
				return
			}
			revs[i] = res.Revision
		}(i)
	}
	wg.Wait()

	sort.Slice(revs, func(i, j int) bool { return revs[i] < revs[j] })
	for i, r := range revs {
		if want := int64(i + 2); r != want {
			t.Fatalf("sorted revisions[%d] = %d, want %d (every commit exactly one revision)", i, r, want)
		}
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != int64(n+1) {
		t.Fatalf("status revision = %d, want %d", st.Revision, n+1)
	}
	if st.Renditions["a"].Head != int64(n-1) {
		t.Fatalf("head = %d, want %d", st.Renditions["a"].Head, n-1)
	}
}

func TestConcurrentDuplicateSubmissionCommitsOnce(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")

	const n = 32
	results := make([]coord.IngestResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := c.Ingest("s1", batch("sub-1", "a", 0, 0, 1, 2))
			if err != nil {
				t.Errorf("Ingest: %v", err)
				return
			}
			results[i] = res
		}(i)
	}
	wg.Wait()

	commits := 0
	for _, res := range results {
		if res.Revision != 2 {
			t.Fatalf("revision = %d, want 2 for every racing caller", res.Revision)
		}
		if !res.Replayed {
			commits++
		}
	}
	if commits != 1 {
		t.Fatalf("commits = %d, want exactly 1 (rest must be replays)", commits)
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 2 {
		t.Fatalf("status revision = %d, want 2", st.Revision)
	}
}
