package httpapi_test

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
)

func itoaInt(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

func TestCutoverHappyPathHTTP(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})
	for i := 0; i <= 4; i++ {
		do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
			"rendition": "a", "generation": 1, "submissionId": "a-" + itoaInt(i),
			"segments": []any{segMap(i, 1000, sha1s)},
		})
	}
	_, st := do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	rev := int(st["revision"].(float64))

	resp, body := do(t, srv, http.MethodPost, "/v1/streams/s/cutover", map[string]any{
		"cutoverId": "co-1", "expectedRevision": rev, "generation": 2,
		"renditions": []string{"a"}, "splicePoints": map[string]any{"a": 5},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("cutover status = %d body=%v", resp.StatusCode, body)
	}
	status := body["status"].(map[string]any)
	if status["currentGeneration"].(float64) != 2 {
		t.Fatalf("current generation should be 2")
	}
	if status["watermark"] != nil {
		t.Fatalf("watermark should be null after cutover")
	}
	rend := status["renditions"].(map[string]any)["a"].(map[string]any)
	if rend["splicePoint"].(float64) != 5 {
		t.Fatalf("splice point should be 5, got %v", rend["splicePoint"])
	}
	if rend["head"] != nil {
		t.Fatalf("head should be null right after cutover")
	}
}

// Scenario: lost cutover response -> verbatim retry must not switch again.
func TestCutoverLostResponseRetryHTTP(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "a0", "segments": []any{segMap(0, 1000, sha1s)},
	})
	body := map[string]any{
		"cutoverId": "co", "expectedRevision": 1, "generation": 2,
		"renditions": []string{"a"}, "splicePoints": map[string]any{"a": 1},
	}
	resp1, b1 := do(t, srv, http.MethodPost, "/v1/streams/s/cutover", body)
	if resp1.StatusCode != http.StatusCreated || b1["applied"] != true {
		t.Fatalf("first cutover: status=%d applied=%v", resp1.StatusCode, b1["applied"])
	}
	rev1 := b1["status"].(map[string]any)["revision"].(float64)

	// Retry verbatim -> 200, not applied, revision unchanged.
	resp2, b2 := do(t, srv, http.MethodPost, "/v1/streams/s/cutover", body)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("retry should be 200, got %d", resp2.StatusCode)
	}
	if b2["applied"] != false {
		t.Fatalf("retry must not apply")
	}
	if b2["status"].(map[string]any)["revision"].(float64) != rev1 {
		t.Fatalf("retry must not bump revision")
	}
}

// Scenario: same cutoverId with drifted content -> stable conflict.
func TestCutoverContentDriftHTTP(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "a0", "segments": []any{segMap(0, 1000, sha1s)},
	})
	do(t, srv, http.MethodPost, "/v1/streams/s/cutover", map[string]any{
		"cutoverId": "co", "expectedRevision": 1, "generation": 2,
		"renditions": []string{"a"}, "splicePoints": map[string]any{"a": 1},
	})
	resp, body := do(t, srv, http.MethodPost, "/v1/streams/s/cutover", map[string]any{
		"cutoverId": "co", "expectedRevision": 1, "generation": 2,
		"renditions": []string{"a"}, "splicePoints": map[string]any{"a": 2}, // drift
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("drift should be 409, got %d", resp.StatusCode)
	}
	if body["code"] != "conflict" {
		t.Fatalf("expected conflict code, got %v", body["code"])
	}
}

// Scenario: stale expectedRevision, generation regression, invalid set.
func TestCutoverStableConflictsHTTP(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "a0", "segments": []any{segMap(0, 1000, sha1s)},
	})
	// revision is now 1.
	cases := []map[string]any{
		{"cutoverId": "c1", "expectedRevision": 0, "generation": 2, "renditions": []string{"a"}, "splicePoints": map[string]any{"a": 1}},      // stale
		{"cutoverId": "c2", "expectedRevision": 1, "generation": 1, "renditions": []string{"a"}, "splicePoints": map[string]any{"a": 1}},      // regression
		{"cutoverId": "c3", "expectedRevision": 1, "generation": 2, "renditions": []string{"a", "b"}, "splicePoints": map[string]any{"a": 1}}, // missing splice
	}
	for i, c := range cases {
		resp, _ := do(t, srv, http.MethodPost, "/v1/streams/s/cutover", c)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("case %d expected 409, got %d", i, resp.StatusCode)
		}
	}
	// Empty cutoverId is a validation error (400), distinct style.
	resp, _ := do(t, srv, http.MethodPost, "/v1/streams/s/cutover", map[string]any{
		"cutoverId": "", "expectedRevision": 1, "generation": 2, "renditions": []string{"a"}, "splicePoints": map[string]any{"a": 1},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty cutoverId should be 400, got %d", resp.StatusCode)
	}
}

// Scenario: late old-generation writes are fenced with no side effects.
func TestLateOldGenerationFencedHTTP(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "a0", "segments": []any{segMap(0, 1000, sha1s)},
	})
	do(t, srv, http.MethodPost, "/v1/streams/s/cutover", map[string]any{
		"cutoverId": "co", "expectedRevision": 1, "generation": 2,
		"renditions": []string{"a"}, "splicePoints": map[string]any{"a": 1},
	})
	_, st := do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	revBefore := st["revision"].(float64)

	// Late gen-1 report.
	resp, body := do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "late", "segments": []any{segMap(1, 1000, sha1s)},
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("late old-gen report should be 409, got %d", resp.StatusCode)
	}
	if body["code"] != "fenced" {
		t.Fatalf("expected fenced code, got %v", body["code"])
	}
	_, st = do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	if st["revision"].(float64) != revBefore {
		t.Fatalf("fenced report must not change revision")
	}
}

// Scenario: partial new-primary readiness. After cutover with two renditions,
// the watermark stays null until BOTH advance past their splice points.
func TestPartialNewPrimaryReadinessHTTP(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a", "b"}})
	for _, r := range []string{"a", "b"} {
		do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
			"rendition": r, "generation": 1, "submissionId": r + "0", "segments": []any{segMap(0, 1000, sha1s)},
		})
	}
	_, st := do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	rev := int(st["revision"].(float64))
	do(t, srv, http.MethodPost, "/v1/streams/s/cutover", map[string]any{
		"cutoverId": "co", "expectedRevision": rev, "generation": 2,
		"renditions": []string{"a", "b"}, "splicePoints": map[string]any{"a": 1, "b": 5},
	})

	// Only a advances past its splice -> watermark null.
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 2, "submissionId": "a-g2-1", "segments": []any{segMap(1, 1000, sha1s)},
	})
	_, st = do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	if st["watermark"] != nil {
		t.Fatalf("watermark should be null while b not ready, got %v", st["watermark"])
	}
	// Now b advances -> watermark = min(head_a=1, head_b=5) = 1.
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "b", "generation": 2, "submissionId": "b-g2-5", "segments": []any{segMap(5, 1000, sha1s)},
	})
	_, st = do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	if st["watermark"] == nil || st["watermark"].(float64) != 1 {
		t.Fatalf("watermark should be 1, got %v", st["watermark"])
	}
}

// Scenario: concurrent cutovers. Many clients race with distinct cutoverIds
// and the same expectedRevision; exactly one must win, the rest conflict, and
// the stream must land on a single coherent generation.
func TestConcurrentCutoversHTTP(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "a0", "segments": []any{segMap(0, 1000, sha1s)},
	})
	// revision == 1 now.

	const n = 30
	var wg sync.WaitGroup
	var created, conflicts int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			resp, _ := do(t, srv, http.MethodPost, "/v1/streams/s/cutover", map[string]any{
				"cutoverId": "co-" + itoaInt(i), "expectedRevision": 1, "generation": uint64(2 + i),
				"renditions": []string{"a"}, "splicePoints": map[string]any{"a": 1},
			})
			switch resp.StatusCode {
			case http.StatusCreated:
				atomic.AddInt64(&created, 1)
			case http.StatusConflict:
				atomic.AddInt64(&conflicts, 1)
			default:
				t.Errorf("unexpected status %d", resp.StatusCode)
			}
		}(i)
	}
	wg.Wait()

	if created != 1 {
		t.Fatalf("exactly one cutover must win, got %d", created)
	}
	if conflicts != n-1 {
		t.Fatalf("expected %d conflicts, got %d", n-1, conflicts)
	}
	// Final revision must be exactly 2 (one segment + one cutover).
	_, st := do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	if st["revision"].(float64) != 2 {
		t.Fatalf("final revision should be 2, got %v", st["revision"])
	}
}
