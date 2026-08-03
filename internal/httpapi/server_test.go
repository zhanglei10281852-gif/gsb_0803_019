package httpapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gsb/coordinator/internal/httpapi"
	"github.com/gsb/coordinator/internal/service"
	"github.com/gsb/coordinator/internal/storage"
)

const sha1s = "1111111111111111111111111111111111111111111111111111111111111111"
const sha2s = "2222222222222222222222222222222222222222222222222222222222222222"

func newServer() *httptest.Server {
	coord := service.New(storage.New())
	return httptest.NewServer(httpapi.New(coord))
}

func do(t *testing.T, srv *httptest.Server, method, path string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, srv.URL+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	var out map[string]any
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatalf("decode response %q: %v", string(data), err)
		}
	}
	return resp, out
}

func segMap(seq, dur int, sha string) map[string]any {
	return map[string]any{"sequence": seq, "durationMs": dur, "sha256": sha}
}

func TestHealthz(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	resp, body := do(t, srv, http.MethodGet, "/healthz", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if body["status"] != "ok" {
		t.Fatalf("body = %v", body)
	}
}

func TestCreateAndStatus(t *testing.T) {
	srv := newServer()
	defer srv.Close()

	resp, body := do(t, srv, http.MethodPost, "/v1/streams", map[string]any{
		"streamId":   "s1",
		"renditions": []string{"720p", "1080p"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d body=%v", resp.StatusCode, body)
	}
	if body["revision"].(float64) != 0 {
		t.Fatalf("initial revision should be 0, got %v", body["revision"])
	}
	if body["watermark"] != nil {
		t.Fatalf("watermark should be null initially, got %v", body["watermark"])
	}

	// Idempotent re-create -> 200.
	resp, _ = do(t, srv, http.MethodPost, "/v1/streams", map[string]any{
		"streamId":   "s1",
		"renditions": []string{"1080p", "720p"},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("re-create status = %d", resp.StatusCode)
	}

	// Conflicting declaration -> 409.
	resp, _ = do(t, srv, http.MethodPost, "/v1/streams", map[string]any{
		"streamId":   "s1",
		"renditions": []string{"720p"},
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("conflicting declaration status = %d", resp.StatusCode)
	}
}

func TestSubmitFlowHeadAndWatermark(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{
		"streamId": "s", "renditions": []string{"a", "b"},
	})

	// a: seq 0,1 (contiguous). b: seq 0 only.
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "a01",
		"segments": []any{segMap(0, 1000, sha1s), segMap(1, 1000, sha1s)},
	})
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "b", "generation": 1, "submissionId": "b0",
		"segments": []any{segMap(0, 1000, sha1s)},
	})

	_, body := do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	if body["revision"].(float64) != 2 {
		t.Fatalf("revision should be 2, got %v", body["revision"])
	}
	// watermark = min(head_a=1, head_b=0) = 0.
	if body["watermark"] == nil || body["watermark"].(float64) != 0 {
		t.Fatalf("watermark should be 0, got %v", body["watermark"])
	}
	rends := body["renditions"].(map[string]any)
	if rends["a"].(map[string]any)["head"].(float64) != 1 {
		t.Fatalf("head a should be 1")
	}
}

func TestOutOfOrderBufferedNoHead(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})

	// Submit seq 2 first (hole at 0,1).
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "s2",
		"segments": []any{segMap(2, 1000, sha1s)},
	})
	_, body := do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	rend := body["renditions"].(map[string]any)["a"].(map[string]any)
	if rend["head"] != nil {
		t.Fatalf("head should be null with hole at 0, got %v", rend["head"])
	}
	buffered := rend["buffered"].([]any)
	if len(buffered) != 1 || buffered[0].(float64) != 2 {
		t.Fatalf("buffered should be [2], got %v", buffered)
	}
	if body["watermark"] != nil {
		t.Fatalf("watermark should be null, got %v", body["watermark"])
	}
}

func TestIdempotentReplayHTTP(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})

	batch := map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "sub",
		"segments": []any{segMap(0, 1000, sha1s)},
	}
	resp1, b1 := do(t, srv, http.MethodPost, "/v1/streams/s/segments", batch)
	if resp1.StatusCode != http.StatusCreated {
		t.Fatalf("first submit = %d", resp1.StatusCode)
	}
	if b1["applied"] != true {
		t.Fatalf("first submit should be applied")
	}
	rev1 := b1["status"].(map[string]any)["revision"].(float64)

	resp2, b2 := do(t, srv, http.MethodPost, "/v1/streams/s/segments", batch)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("replay should be 200, got %d", resp2.StatusCode)
	}
	if b2["applied"] != false {
		t.Fatalf("replay should not be applied")
	}
	rev2 := b2["status"].(map[string]any)["revision"].(float64)
	if rev1 != rev2 {
		t.Fatalf("replay must not bump revision: %v vs %v", rev1, rev2)
	}
}

func TestSameSubmissionConflictHTTP(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "sub",
		"segments": []any{segMap(0, 1000, sha1s)},
	})
	resp, body := do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "sub",
		"segments": []any{segMap(0, 1000, sha2s)}, // different content
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}
	if body["code"] != "conflict" {
		t.Fatalf("expected conflict code, got %v", body["code"])
	}
}

func TestBatchAtomicityHTTP(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})
	// Commit seq 5.
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "first",
		"segments": []any{segMap(5, 1000, sha1s)},
	})
	// Batch with new seq 0 + conflicting rewrite of seq 5 -> must fail wholesale.
	resp, _ := do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "second",
		"segments": []any{segMap(0, 1000, sha1s), segMap(5, 1000, sha2s)},
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}
	// seq 0 must not have been written.
	_, body := do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	rend := body["renditions"].(map[string]any)["a"].(map[string]any)
	if rend["head"] != nil {
		t.Fatalf("partial write: head should be null, got %v", rend["head"])
	}
	if rend["segmentCount"].(float64) != 1 {
		t.Fatalf("only 1 segment should exist, got %v", rend["segmentCount"])
	}
	if body["revision"].(float64) != 1 {
		t.Fatalf("revision should still be 1, got %v", body["revision"])
	}
}

func TestSubmitToMissingStream(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	resp, _ := do(t, srv, http.MethodPost, "/v1/streams/ghost/segments", map[string]any{
		"rendition": "a", "generation": 1, "submissionId": "x",
		"segments": []any{segMap(0, 1000, sha1s)},
	})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestValidationErrors(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})

	cases := []map[string]any{
		{"rendition": "a", "generation": 1, "submissionId": "x", "segments": []any{}},                           // empty batch
		{"rendition": "a", "generation": 1, "submissionId": "x", "segments": []any{segMap(0, 0, sha1s)}},        // zero duration
		{"rendition": "a", "generation": 1, "submissionId": "x", "segments": []any{segMap(0, 1000, "bad")}},     // bad sha
		{"rendition": "ghost", "generation": 1, "submissionId": "x", "segments": []any{segMap(0, 1000, sha1s)}}, // unknown rendition
		{"rendition": "a", "generation": 1, "submissionId": "", "segments": []any{segMap(0, 1000, sha1s)}},      // empty submissionId
	}
	for i, c := range cases {
		resp, _ := do(t, srv, http.MethodPost, "/v1/streams/s/segments", c)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("case %d: expected 400, got %d", i, resp.StatusCode)
		}
	}
}

func TestNewGenerationResetsWatermark(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a", "b"}})
	// gen 1 complete for both.
	for _, r := range []string{"a", "b"} {
		do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
			"rendition": r, "generation": 1, "submissionId": r + "g1",
			"segments": []any{segMap(0, 1000, sha1s)},
		})
	}
	_, body := do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	if body["watermark"].(float64) != 0 {
		t.Fatalf("gen1 watermark should be 0")
	}
	// gen 2 for only a -> current gen 2, watermark null.
	do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
		"rendition": "a", "generation": 2, "submissionId": "ag2",
		"segments": []any{segMap(0, 1000, sha1s)},
	})
	_, body = do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	if body["currentGeneration"].(float64) != 2 {
		t.Fatalf("current generation should be 2, got %v", body["currentGeneration"])
	}
	if body["watermark"] != nil {
		t.Fatalf("watermark should reset to null, got %v", body["watermark"])
	}
}

// TestConcurrentSubmissionsRevisionConsistency drives many concurrent distinct
// submissions through real HTTP and asserts the final revision equals the
// number of applied writes, and observed revisions are unique (serializable).
func TestConcurrentSubmissionsRevisionConsistency(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})

	const n = 60
	var wg sync.WaitGroup
	revs := make([]float64, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, body := do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
				"rendition": "a", "generation": 1, "submissionId": fmt.Sprintf("sub-%d", i),
				"segments": []any{segMap(i, 1000, sha1s)},
			})
			revs[i] = body["status"].(map[string]any)["revision"].(float64)
		}(i)
	}
	wg.Wait()

	seen := make(map[float64]bool)
	for _, r := range revs {
		if seen[r] {
			t.Fatalf("duplicate revision observed: %v", r)
		}
		seen[r] = true
	}
	_, body := do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	if body["revision"].(float64) != n {
		t.Fatalf("final revision should be %d, got %v", n, body["revision"])
	}
}

// TestConcurrentReplayHTTP fires the identical submission concurrently; the
// stream must end at revision 1 regardless of interleaving.
func TestConcurrentReplayHTTP(t *testing.T) {
	srv := newServer()
	defer srv.Close()
	do(t, srv, http.MethodPost, "/v1/streams", map[string]any{"streamId": "s", "renditions": []string{"a"}})

	const n = 40
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			do(t, srv, http.MethodPost, "/v1/streams/s/segments", map[string]any{
				"rendition": "a", "generation": 1, "submissionId": "same",
				"segments": []any{segMap(0, 1000, sha1s)},
			})
		}()
	}
	wg.Wait()
	_, body := do(t, srv, http.MethodGet, "/v1/streams/s", nil)
	if body["revision"].(float64) != 1 {
		t.Fatalf("idempotent replays must leave revision at 1, got %v", body["revision"])
	}
}
