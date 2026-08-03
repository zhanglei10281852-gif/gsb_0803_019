package service

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gsb/coordinator/internal/domain"
	"github.com/gsb/coordinator/internal/storage"
)

const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func newCoord() *Coordinator { return New(storage.New()) }

func TestCreateStreamIdempotent(t *testing.T) {
	c := newCoord()
	_, created, err := c.CreateStream(CreateStreamRequest{StreamID: "s", Renditions: []string{"a", "b"}})
	if err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}
	_, created2, err := c.CreateStream(CreateStreamRequest{StreamID: "s", Renditions: []string{"b", "a"}})
	if err != nil || created2 {
		t.Fatalf("re-declare should be idempotent: created=%v err=%v", created2, err)
	}
	_, _, err = c.CreateStream(CreateStreamRequest{StreamID: "s", Renditions: []string{"a"}})
	if domain.KindOf(err) != domain.KindAlreadyExists {
		t.Fatalf("different declaration should conflict, got %v", err)
	}
}

func TestSubmitUnknownStream(t *testing.T) {
	c := newCoord()
	_, err := c.Submit(SubmitRequest{StreamID: "missing", Rendition: "a", Generation: 1, SubmissionID: "x",
		Segments: []domain.Segment{{Sequence: 0, DurationMs: 1, SHA256: sha}}})
	if domain.KindOf(err) != domain.KindNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

// TestConcurrentReplayIdempotent fires the same submission from many goroutines.
// Exactly one must apply; all reported revisions must be consistent with a
// serial order (here: all equal to 1).
func TestConcurrentReplayIdempotent(t *testing.T) {
	c := newCoord()
	c.CreateStream(CreateStreamRequest{StreamID: "s", Renditions: []string{"a"}})

	const n = 50
	var wg sync.WaitGroup
	applied := make([]bool, n)
	revs := make([]uint64, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			resp, err := c.Submit(SubmitRequest{StreamID: "s", Rendition: "a", Generation: 1, SubmissionID: "same",
				Segments: []domain.Segment{{Sequence: 0, DurationMs: 1000, SHA256: sha}}})
			errs[i] = err
			if err == nil {
				applied[i] = resp.Result.Applied
				revs[i] = resp.Result.Revision
			}
		}(i)
	}
	wg.Wait()

	appliedCount := 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("unexpected error: %v", errs[i])
		}
		if applied[i] {
			appliedCount++
		}
		if revs[i] != 1 {
			t.Fatalf("all revisions must be 1 (single applied write), got %d", revs[i])
		}
	}
	if appliedCount != 1 {
		t.Fatalf("exactly one submission must apply, got %d", appliedCount)
	}
	snap, _ := c.Status("s")
	if snap.Revision != 1 {
		t.Fatalf("final revision should be 1, got %d", snap.Revision)
	}
}

// TestConcurrentDistinctSubmissions verifies revisions form a serial sequence
// 1..n with no gaps or duplicates.
func TestConcurrentDistinctSubmissions(t *testing.T) {
	c := newCoord()
	c.CreateStream(CreateStreamRequest{StreamID: "s", Renditions: []string{"a"}})

	const n = 40
	var wg sync.WaitGroup
	revs := make([]uint64, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			resp, err := c.Submit(SubmitRequest{StreamID: "s", Rendition: "a", Generation: 1,
				SubmissionID: "sub-" + itoa(uint64(i)),
				Segments:     []domain.Segment{{Sequence: uint64(i), DurationMs: 1000, SHA256: sha}}})
			if err != nil {
				t.Errorf("submit %d: %v", i, err)
				return
			}
			revs[i] = resp.Result.Revision
		}(i)
	}
	wg.Wait()

	seen := make(map[uint64]bool)
	for _, r := range revs {
		if r < 1 || r > n {
			t.Fatalf("revision out of range: %d", r)
		}
		if seen[r] {
			t.Fatalf("duplicate revision %d", r)
		}
		seen[r] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d distinct revisions, got %d", n, len(seen))
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

// TestConcurrentCutoverVsLateWrites races a cutover against concurrent old
// generation writes. Because the cutover carries a fixed expectedRevision, it
// either linearizes before the late writes (and applies) or loses to them
// (optimistic-concurrency conflict). Whichever happens, the result must be
// explainable by a single serial order: if the cutover applied, the stream is
// on generation 2 and no further gen-1 write can ever apply (fenced); if it did
// not, the stream stays on generation 1.
func TestConcurrentCutoverVsLateWrites(t *testing.T) {
	c := newCoord()
	c.CreateStream(CreateStreamRequest{StreamID: "s", Renditions: []string{"a"}})
	// Seed gen 1 with seq 0 -> revision 1.
	c.Submit(SubmitRequest{StreamID: "s", Rendition: "a", Generation: 1, SubmissionID: "seed",
		Segments: []domain.Segment{{Sequence: 0, DurationMs: 1000, SHA256: sha}}})

	var wg sync.WaitGroup
	cutApplied := make(chan bool, 1)
	// One cutover to gen 2, splice at 1, guarded by expectedRevision 1.
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := c.Cutover(CutoverRequest{StreamID: "s", CutoverID: "co", ExpectedRevision: 1, Generation: 2,
			Renditions: []string{"a"}, SplicePoints: map[string]uint64{"a": 1}})
		if err != nil {
			cutApplied <- false
			return
		}
		cutApplied <- resp.Result.Applied
	}()
	// Many concurrent late gen-1 writes; each either lands before the cutover
	// (revision advances) or is fenced afterward.
	const n = 30
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			c.Submit(SubmitRequest{StreamID: "s", Rendition: "a", Generation: 1,
				SubmissionID: "late-" + itoa(uint64(i)),
				Segments:     []domain.Segment{{Sequence: uint64(1 + i), DurationMs: 1000, SHA256: sha}}})
		}(i)
	}
	wg.Wait()

	applied := <-cutApplied
	snap, _ := c.Status("s")
	if applied {
		if snap.CurrentGeneration == nil || *snap.CurrentGeneration != 2 {
			t.Fatalf("cutover applied: stream must be on generation 2, got %v", snap.CurrentGeneration)
		}
		// No gen-1 write can apply after the linearized cutover.
		_, err := c.Submit(SubmitRequest{StreamID: "s", Rendition: "a", Generation: 1, SubmissionID: "post",
			Segments: []domain.Segment{{Sequence: 99, DurationMs: 1000, SHA256: sha}}})
		if domain.KindOf(err) != domain.KindFenced {
			t.Fatalf("post-cutover gen-1 write must be fenced, got %v", err)
		}
	} else {
		if snap.CurrentGeneration == nil || *snap.CurrentGeneration != 1 {
			t.Fatalf("cutover lost: stream must remain on generation 1, got %v", snap.CurrentGeneration)
		}
	}
}

// TestConcurrentCutoversSingleWinner fires many distinct cutoverIds at the same
// expectedRevision with no competing writes: exactly one must apply and the
// rest must conflict, and the final revision advances by exactly one.
func TestConcurrentCutoversSingleWinner(t *testing.T) {
	c := newCoord()
	c.CreateStream(CreateStreamRequest{StreamID: "s", Renditions: []string{"a"}})
	c.Submit(SubmitRequest{StreamID: "s", Rendition: "a", Generation: 1, SubmissionID: "seed",
		Segments: []domain.Segment{{Sequence: 0, DurationMs: 1000, SHA256: sha}}})
	// revision == 1.

	const n = 40
	var wg sync.WaitGroup
	var applied, conflicts int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			resp, err := c.Cutover(CutoverRequest{StreamID: "s", CutoverID: "co-" + itoa(uint64(i)),
				ExpectedRevision: 1, Generation: uint64(2 + i),
				Renditions: []string{"a"}, SplicePoints: map[string]uint64{"a": 1}})
			if err != nil {
				if domain.KindOf(err) != domain.KindConflict {
					t.Errorf("unexpected error kind: %v", err)
				}
				atomic.AddInt64(&conflicts, 1)
				return
			}
			if resp.Result.Applied {
				atomic.AddInt64(&applied, 1)
			}
		}(i)
	}
	wg.Wait()

	if applied != 1 {
		t.Fatalf("exactly one cutover must apply, got %d", applied)
	}
	if conflicts != n-1 {
		t.Fatalf("expected %d conflicts, got %d", n-1, conflicts)
	}
	snap, _ := c.Status("s")
	if snap.Revision != 2 {
		t.Fatalf("final revision should be 2, got %d", snap.Revision)
	}
}
