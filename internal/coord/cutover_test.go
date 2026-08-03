package coord_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"livecoord/internal/coord"
)

func mustCutover(t *testing.T, c *coord.Coordinator, stream string, req coord.CutoverRequest) coord.CutoverResult {
	t.Helper()
	res, err := c.Cutover(stream, req)
	if err != nil {
		t.Fatalf("Cutover(%s cutover=%s gen=%d): %v", stream, req.CutoverID, req.Generation, err)
	}
	return res
}

func cutoverReq(id string, expectedRev, gen int64, resumes ...coord.RenditionResume) coord.CutoverRequest {
	return coord.CutoverRequest{
		CutoverID:        id,
		ExpectedRevision: expectedRev,
		Generation:       gen,
		Renditions:       resumes,
	}
}

func resume(name string, start int64) coord.RenditionResume {
	return coord.RenditionResume{Name: name, StartSequence: start}
}

func TestCutoverSwitchesGenerationWithResumePoints(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a", "b")
	mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0, 1))
	mustIngest(t, c, "s1", batch("sub-2", "b", 0, 0)) // revision 3

	res := mustCutover(t, c, "s1", cutoverReq("co-1", 3, 5, resume("a", 100), resume("b", 98)))
	if res.Replayed {
		t.Fatal("first cutover must not be marked replayed")
	}
	if res.Revision != 4 || res.Generation != 5 {
		t.Fatalf("cutover = %+v, want revision 4 generation 5", res)
	}
	if res.Watermark != 97 {
		t.Fatalf("watermark = %d, want 97 (min of resume points -1)", res.Watermark)
	}
	if res.Renditions["a"].Head != 99 || res.Renditions["b"].Head != 97 {
		t.Fatalf("renditions = %+v, want heads 99/97", res.Renditions)
	}

	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 4 || st.Generation != 5 || st.Watermark != 97 {
		t.Fatalf("status = %+v", st)
	}
	if st.Renditions["a"].Buffered != 0 || st.Renditions["b"].Buffered != 0 {
		t.Fatalf("old generation segments must be discarded: %+v", st.Renditions)
	}
}

func TestCutoverNewGenerationDoesNotBorrowOldProgress(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0, 1, 2, 3)) // old head 3
	mustIngest(t, c, "s1", batch("sub-2", "a", 0, 10, 11))     // old buffered 10,11
	mustCutover(t, c, "s1", cutoverReq("co-1", 3, 1, resume("a", 100)))

	// New generation starts exactly at the resume point: old head (3) and
	// old buffered segments (10, 11) must not leak into it.
	res := mustIngest(t, c, "s1", batch("sub-3", "a", 1, 101, 102))
	if res.Head != 99 {
		t.Fatalf("head = %d, want 99 (gap at 100; old progress not borrowed)", res.Head)
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Renditions["a"].Buffered != 2 {
		t.Fatalf("buffered = %d, want 2 (only new-generation 101, 102)", st.Renditions["a"].Buffered)
	}

	res = mustIngest(t, c, "s1", batch("sub-4", "a", 1, 100))
	if res.Head != 102 {
		t.Fatalf("head = %d, want 102 after filling the gap", res.Head)
	}
}

func TestCutoverPartialNewPrimaryReadiness(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a", "b")
	mustCutover(t, c, "s1", cutoverReq("co-1", 1, 5, resume("a", 100), resume("b", 200)))

	// Only one new-primary rendition is ready: watermark is still bounded by
	// the other's resume point.
	res := mustIngest(t, c, "s1", batch("sub-1", "a", 5, 100, 101))
	if res.Watermark != 101 {
		t.Fatalf("watermark = %d, want 101 (b already resumed at 200)", res.Watermark)
	}

	// Out-of-order arrival on b does not cross the gap at 200.
	res = mustIngest(t, c, "s1", batch("sub-2", "b", 5, 201))
	if res.Head != 199 || res.Watermark != 101 {
		t.Fatalf("b = head %d watermark %d, want head 199 watermark 101", res.Head, res.Watermark)
	}

	res = mustIngest(t, c, "s1", batch("sub-3", "b", 5, 200))
	if res.Head != 201 || res.Watermark != 101 {
		t.Fatalf("b = head %d watermark %d, want head 201 watermark 101", res.Head, res.Watermark)
	}

	res = mustIngest(t, c, "s1", batch("sub-4", "a", 5, 102))
	if res.Watermark != 102 {
		t.Fatalf("watermark = %d, want 102", res.Watermark)
	}
}

func TestCutoverFencesLateOldGenerationWrites(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	orig := mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0, 1)) // revision 2
	mustCutover(t, c, "s1", cutoverReq("co-1", 2, 3, resume("a", 100)))

	// A brand-new submission on the old generation is fenced without side
	// effects: no revision bump, no idempotency record, no watermark change.
	_, err := c.Ingest("s1", batch("sub-late", "a", 0, 2))
	if got := codeOf(t, err); got != coord.CodeStaleGeneration {
		t.Fatalf("code = %q, want %q", got, coord.CodeStaleGeneration)
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 3 || st.Generation != 3 || st.Watermark != 99 || st.Renditions["a"].Buffered != 0 {
		t.Fatalf("status after fenced write = %+v, want untouched", st)
	}

	// Retrying the fenced submission keeps fencing deterministically.
	if _, err := c.Ingest("s1", batch("sub-late", "a", 0, 2)); codeOf(t, err) != coord.CodeStaleGeneration {
		t.Fatalf("fenced retry must stay fenced: %v", err)
	}

	// A retry of a pre-cutover committed submission still replays its
	// recorded result, changing nothing.
	replayed, err := c.Ingest("s1", batch("sub-1", "a", 0, 0, 1))
	if err != nil {
		t.Fatalf("old committed replay: %v", err)
	}
	if !replayed.Replayed || replayed.Revision != orig.Revision {
		t.Fatalf("replay = %+v, want revision %d replayed", replayed, orig.Revision)
	}
	st, err = c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 3 || st.Watermark != 99 {
		t.Fatalf("status after old replay = %+v, want revision 3 watermark 99", st)
	}
}

func TestCutoverExpectedRevisionConflict(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	mustIngest(t, c, "s1", batch("sub-1", "a", 0, 0)) // revision 2

	_, err := c.Cutover("s1", cutoverReq("co-1", 1, 1, resume("a", 100)))
	if got := codeOf(t, err); got != coord.CodeRevisionConflict {
		t.Fatalf("code = %q, want %q", got, coord.CodeRevisionConflict)
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 2 || st.Generation != 0 {
		t.Fatalf("status = %+v, want untouched (revision 2 generation 0)", st)
	}

	// Deterministic: the same rejected request conflicts again.
	if _, err := c.Cutover("s1", cutoverReq("co-1", 1, 1, resume("a", 100))); codeOf(t, err) != coord.CodeRevisionConflict {
		t.Fatalf("repeated stale expectedRevision: %v", err)
	}
}

func TestCutoverGenerationRegression(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	mustIngest(t, c, "s1", batch("sub-1", "a", 2, 0)) // generation 2, revision 2

	for _, gen := range []int64{2, 1, 0} {
		_, err := c.Cutover("s1", cutoverReq(fmt.Sprintf("co-%d", gen), 2, gen, resume("a", 100)))
		if got := codeOf(t, err); got != coord.CodeStaleGeneration {
			t.Fatalf("generation %d: code = %q, want %q", gen, got, coord.CodeStaleGeneration)
		}
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 2 || st.Generation != 2 {
		t.Fatalf("status = %+v, want untouched", st)
	}
}

func TestCutoverInvalidRenditionSet(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")

	cases := []struct {
		name string
		req  coord.CutoverRequest
	}{
		{"empty set", cutoverReq("co-1", 1, 1)},
		{"duplicate names", cutoverReq("co-2", 1, 1, resume("a", 1), resume("a", 2))},
		{"negative start", cutoverReq("co-3", 1, 1, resume("a", -1))},
		{"invalid name", cutoverReq("co-4", 1, 1, resume("bad/name", 1))},
		{"missing cutoverId", cutoverReq("", 1, 1, resume("a", 1))},
		{"zero expectedRevision", cutoverReq("co-5", 0, 1, resume("a", 1))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := c.Cutover("s1", tc.req); codeOf(t, err) != coord.CodeInvalidArgument {
				t.Fatalf("code = %v, want %q", err, coord.CodeInvalidArgument)
			}
		})
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 1 || st.Generation != 0 {
		t.Fatalf("status = %+v, want untouched", st)
	}
}

func TestCutoverReplayAfterLostResponse(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	req := cutoverReq("co-1", 1, 5, resume("a", 100))

	first := mustCutover(t, c, "s1", req)
	second := mustCutover(t, c, "s1", req)
	if !second.Replayed {
		t.Fatal("retried cutover must be marked replayed")
	}
	if second.Revision != first.Revision || second.Watermark != first.Watermark || second.Generation != first.Generation {
		t.Fatalf("replay = %+v, want original result %+v", second, first)
	}

	// The switch happened exactly once.
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 2 || st.Generation != 5 {
		t.Fatalf("status = %+v, want revision 2 generation 5", st)
	}

	// Even after the stream moved on, the replay returns the original
	// committed result instead of re-switching or conflicting.
	mustIngest(t, c, "s1", batch("sub-9", "a", 5, 100)) // revision 3
	third := mustCutover(t, c, "s1", req)
	if !third.Replayed || third.Revision != first.Revision {
		t.Fatalf("late replay = %+v, want revision %d replayed", third, first.Revision)
	}
	st, err = c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 3 {
		t.Fatalf("status revision = %d, want 3 (replay must not advance)", st.Revision)
	}
}

func TestCutoverConflictSameIDDifferentContent(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")
	mustCutover(t, c, "s1", cutoverReq("co-1", 1, 5, resume("a", 100)))

	drifts := []coord.CutoverRequest{
		cutoverReq("co-1", 1, 5, resume("a", 101)),                  // resume point drift
		cutoverReq("co-1", 1, 6, resume("a", 100)),                  // generation drift
		cutoverReq("co-1", 1, 5, resume("a", 100), resume("b", 50)), // set drift
	}
	for _, req := range drifts {
		if _, err := c.Cutover("s1", req); codeOf(t, err) != coord.CodeCutoverConflict {
			t.Fatalf("drift %+v: %v, want %q", req.Renditions, err, coord.CodeCutoverConflict)
		}
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 2 || st.Generation != 5 {
		t.Fatalf("status = %+v, want untouched", st)
	}
}

func TestConcurrentCutoversHaveSingleWinner(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")

	const n = 16
	results := make([]coord.CutoverResult, n)
	codes := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := c.Cutover("s1", cutoverReq(fmt.Sprintf("co-%d", i), 1, int64(10+i), resume("a", 100)))
			if err != nil {
				var ce *coord.Error
				if !asCoordError(err, &ce) {
					t.Errorf("unexpected error: %v", err)
					return
				}
				codes[i] = ce.Code
				return
			}
			results[i] = res
			codes[i] = "ok"
		}(i)
	}
	wg.Wait()

	winnerGen := int64(-1)
	wins := 0
	for i, code := range codes {
		switch code {
		case "ok":
			wins++
			winnerGen = results[i].Generation
			if results[i].Revision != 2 {
				t.Fatalf("winner revision = %d, want 2", results[i].Revision)
			}
		case coord.CodeRevisionConflict:
			// expected loser outcome
		default:
			t.Fatalf("goroutine %d: unexpected code %q", i, code)
		}
	}
	if wins != 1 {
		t.Fatalf("wins = %d, want exactly 1", wins)
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 2 || st.Generation != winnerGen {
		t.Fatalf("status = %+v, want revision 2 generation %d", st, winnerGen)
	}
}

func TestConcurrentDuplicateCutoverCommitsOnce(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")

	const n = 16
	results := make([]coord.CutoverResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := c.Cutover("s1", cutoverReq("co-1", 1, 5, resume("a", 100)))
			if err != nil {
				t.Errorf("Cutover: %v", err)
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
}

func TestCutoverRacesInFlightOldWrite(t *testing.T) {
	c := newCoordinator()
	mustCreate(t, c, "s1", "a")

	var wg sync.WaitGroup
	ingestOK := false
	cutoverOK := false
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := c.Ingest("s1", batch("sub-race", "a", 0, 0))
		if err == nil {
			ingestOK = true
			return
		}
		if got := codeOf(t, err); got != coord.CodeStaleGeneration {
			t.Errorf("ingest code = %q, want %q", got, coord.CodeStaleGeneration)
		}
	}()
	go func() {
		defer wg.Done()
		_, err := c.Cutover("s1", cutoverReq("co-1", 1, 5, resume("a", 100)))
		if err == nil {
			cutoverOK = true
			return
		}
		if got := codeOf(t, err); got != coord.CodeRevisionConflict {
			t.Errorf("cutover code = %q, want %q", got, coord.CodeRevisionConflict)
		}
	}()
	wg.Wait()

	// The linearization point is the lock: either the old write completed
	// entirely before the switch (cutover then conflicts on revision) or the
	// switch happened first (old write is fenced without side effects).
	if ingestOK == cutoverOK {
		t.Fatalf("ingestOK=%v cutoverOK=%v, want exactly one winner", ingestOK, cutoverOK)
	}
	st, err := c.Status("s1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Revision != 2 {
		t.Fatalf("revision = %d, want exactly one commit", st.Revision)
	}
	if cutoverOK && (st.Generation != 5 || st.Watermark != 99) {
		t.Fatalf("cutover won but status = %+v", st)
	}
	if ingestOK && (st.Generation != 0 || st.Renditions["a"].Head != 0) {
		t.Fatalf("ingest won but status = %+v", st)
	}
}

func asCoordError(err error, target **coord.Error) bool {
	return errors.As(err, target)
}
