package service

import (
	"sync"
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
