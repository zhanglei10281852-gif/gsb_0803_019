package httpapi_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"livecoord/internal/domain"
	"livecoord/internal/httpapi"
	"livecoord/internal/store"
)

func testHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func seg(seq int64, dur int64, content string) domain.Segment {
	return domain.Segment{Sequence: seq, DurationMs: dur, Sha256: testHash(content)}
}

type apiError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type testClient struct {
	t      *testing.T
	server *httptest.Server
}

func newTestClient(t *testing.T) *testClient {
	t.Helper()
	coord := domain.NewCoordinator(store.NewMemoryStore())
	srv := httpapi.NewServer(coord)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &testClient{t: t, server: ts}
}

func (c *testClient) do(method, path string, body interface{}) (*http.Response, []byte) {
	c.t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			c.t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.server.URL+path, rdr)
	if err != nil {
		c.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("do request: %v", err)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatalf("read body: %v", err)
	}
	resp.Body.Close()
	return resp, data
}

func decode[T any](t *testing.T, data []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("unmarshal %s: %v", string(data), err)
	}
	return v
}

func createStream(t *testing.T, c *testClient, id string, renditions []string) domain.StreamView {
	t.Helper()
	resp, body := c.do("POST", "/v1/streams", map[string]interface{}{
		"streamId":         id,
		"activeRenditions": renditions,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create stream status %d: %s", resp.StatusCode, body)
	}
	return decode[domain.StreamView](t, body)
}

func TestHealthEndpoint(t *testing.T) {
	c := newTestClient(t)
	resp, body := c.do("GET", "/healthz", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	var h domain.HealthView
	if err := json.Unmarshal(body, &h); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if h.Status != "ok" {
		t.Fatalf("unexpected health: %+v", h)
	}
}

func TestCreateAndGetStream(t *testing.T) {
	c := newTestClient(t)
	view := createStream(t, c, "s1", []string{"720p", "480p"})
	if view.Revision != 0 || view.Generation != 0 {
		t.Fatalf("unexpected create view: %+v", view)
	}

	resp, body := c.do("GET", "/v1/streams/s1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status %d: %s", resp.StatusCode, body)
	}
	st := decode[domain.StreamView](t, body)
	if st.StreamID != "s1" || len(st.ActiveRenditions) != 2 {
		t.Fatalf("unexpected status: %+v", st)
	}
	if st.Watermark.Sequence != 0 {
		t.Fatalf("watermark should be 0, got %d", st.Watermark.Sequence)
	}
}

func TestCreateDuplicateStreamConflict(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p"})
	resp, body := c.do("POST", "/v1/streams", map[string]interface{}{
		"streamId":         "s1",
		"activeRenditions": []string{"480p"},
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", resp.StatusCode, body)
	}
	e := decode[apiError](t, body)
	if e.Error != domain.ErrCodeStreamExists {
		t.Fatalf("unexpected error code: %s", e.Error)
	}
}

func TestGetStreamNotFound(t *testing.T) {
	c := newTestClient(t)
	resp, body := c.do("GET", "/v1/streams/missing", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", resp.StatusCode, body)
	}
}

func TestSubmitAndWatermarkFlow(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p", "480p"})

	submit := func(rendition, subID string, segs ...domain.Segment) domain.SubmitResultView {
		t.Helper()
		resp, body := c.do("POST", "/v1/streams/s1/segments", map[string]interface{}{
			"rendition":    rendition,
			"generation":   1,
			"submissionId": subID,
			"segments":     segs,
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("submit %s status %d: %s", subID, resp.StatusCode, body)
		}
		return decode[domain.SubmitResultView](t, body)
	}

	r1 := submit("720p", "a1", seg(0, 2000, "a0"), seg(1, 2000, "a1"))
	if r1.Revision != 1 || r1.HeadSequence != 2 {
		t.Fatalf("unexpected r1: %+v", r1)
	}
	if r1.Watermark.Sequence != 0 {
		t.Fatalf("watermark must stay 0 until all renditions present: %+v", r1.Watermark)
	}

	r2 := submit("480p", "b1", seg(0, 2000, "b0"))
	if r2.Watermark.Sequence != 1 || r2.Watermark.TimeMs != 2000 {
		t.Fatalf("watermark should advance to seq=1 time=2000: %+v", r2.Watermark)
	}
	if r2.Revision != 2 {
		t.Fatalf("expected revision 2, got %d", r2.Revision)
	}

	_, body := c.do("GET", "/v1/streams/s1", nil)
	st := decode[domain.StreamView](t, body)
	if st.Revision != 2 {
		t.Fatalf("status revision mismatch: got %d", st.Revision)
	}
	if st.Renditions["720p"].HeadSequence != 2 || st.Renditions["480p"].HeadSequence != 1 {
		t.Fatalf("unexpected rendition heads: %+v", st.Renditions)
	}
	if st.Watermark.Sequence != 1 {
		t.Fatalf("unexpected status watermark: %+v", st.Watermark)
	}
}

func TestOutOfOrderThenFillGap(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p"})

	resp, body := c.do("POST", "/v1/streams/s1/segments", map[string]interface{}{
		"rendition": "720p", "generation": 1, "submissionId": "s1",
		"segments": []domain.Segment{seg(0, 2000, "a0"), seg(2, 2000, "a2")},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	r1 := decode[domain.SubmitResultView](t, body)
	if r1.HeadSequence != 1 || r1.Watermark.Sequence != 1 {
		t.Fatalf("head must not cross gap at seq=1: %+v", r1)
	}

	resp, body = c.do("POST", "/v1/streams/s1/segments", map[string]interface{}{
		"rendition": "720p", "generation": 1, "submissionId": "s2",
		"segments": []domain.Segment{seg(1, 2000, "a1")},
	})
	r2 := decode[domain.SubmitResultView](t, body)
	if r2.HeadSequence != 3 || r2.Watermark.Sequence != 3 || r2.Watermark.TimeMs != 6000 {
		t.Fatalf("head should drain buffered after fill: %+v", r2)
	}
}

func TestIdempotentRetryOverHTTP(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p"})
	payload := map[string]interface{}{
		"rendition": "720p", "generation": 1, "submissionId": "sub-1",
		"segments": []domain.Segment{seg(0, 2000, "a0")},
	}
	resp, body := c.do("POST", "/v1/streams/s1/segments", payload)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	first := decode[domain.SubmitResultView](t, body)
	if first.Revision != 1 || first.Idempotent {
		t.Fatalf("unexpected first: %+v", first)
	}

	resp, body = c.do("POST", "/v1/streams/s1/segments", payload)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry status %d: %s", resp.StatusCode, body)
	}
	retry := decode[domain.SubmitResultView](t, body)
	if !retry.Idempotent || retry.Revision != 1 {
		t.Fatalf("retry must be idempotent at revision 1: %+v", retry)
	}

	resp, body = c.do("GET", "/v1/streams/s1", nil)
	st := decode[domain.StreamView](t, body)
	if st.Revision != 1 {
		t.Fatalf("revision must not advance on retry: %d", st.Revision)
	}
}

func TestSameSubmissionIDDifferentContentConflict(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p"})
	c.do("POST", "/v1/streams/s1/segments", map[string]interface{}{
		"rendition": "720p", "generation": 1, "submissionId": "sub-1",
		"segments": []domain.Segment{seg(0, 2000, "a0")},
	})
	resp, body := c.do("POST", "/v1/streams/s1/segments", map[string]interface{}{
		"rendition": "720p", "generation": 1, "submissionId": "sub-1",
		"segments": []domain.Segment{seg(0, 2000, "tampered")},
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", resp.StatusCode, body)
	}
	e := decode[apiError](t, body)
	if e.Error != domain.ErrCodeConflict {
		t.Fatalf("expected CONFLICT, got %s", e.Error)
	}
}

func TestAtomicBatchNoPartialOnValidationError(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p"})
	resp, body := c.do("POST", "/v1/streams/s1/segments", map[string]interface{}{
		"rendition": "720p", "generation": 1, "submissionId": "bad",
		"segments": []domain.Segment{
			seg(0, 2000, "ok"),
			{Sequence: 1, DurationMs: 2000, Sha256: "not-a-valid-hash"},
		},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
	}

	resp, body = c.do("GET", "/v1/streams/s1", nil)
	st := decode[domain.StreamView](t, body)
	if st.Revision != 0 {
		t.Fatalf("revision must stay 0 after rejected batch, got %d", st.Revision)
	}
	if st.Renditions["720p"].HeadSequence != 0 || st.Renditions["720p"].BufferedSegments != 0 {
		t.Fatalf("no partial state should persist: %+v", st.Renditions["720p"])
	}
}

func TestStaleGenerationConflict(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p"})
	c.do("POST", "/v1/streams/s1/segments", map[string]interface{}{
		"rendition": "720p", "generation": 2, "submissionId": "s1",
		"segments": []domain.Segment{seg(0, 2000, "a0")},
	})
	resp, body := c.do("POST", "/v1/streams/s1/segments", map[string]interface{}{
		"rendition": "720p", "generation": 1, "submissionId": "s2",
		"segments": []domain.Segment{seg(0, 2000, "b0")},
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", resp.StatusCode, body)
	}
	e := decode[apiError](t, body)
	if e.Error != domain.ErrCodeStaleGeneration {
		t.Fatalf("expected STALE_GENERATION, got %s", e.Error)
	}
}

func TestInvalidJSON(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p"})
	req, _ := http.NewRequest("POST", c.server.URL+"/v1/streams/s1/segments", bytes.NewBufferString("{not json"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p"})
	resp, _ := c.do("POST", "/v1/streams/s1/segments", map[string]interface{}{
		"rendition": "720p", "generation": 1, "submissionId": "s1",
		"segments": []domain.Segment{seg(0, 2000, "a0")},
		"bogus":    123,
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown field, got %d", resp.StatusCode)
	}
}

func TestSubmitToMissingStream(t *testing.T) {
	c := newTestClient(t)
	resp, body := c.do("POST", "/v1/streams/nope/segments", map[string]interface{}{
		"rendition": "720p", "generation": 1, "submissionId": "s1",
		"segments": []domain.Segment{seg(0, 2000, "a0")},
	})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", resp.StatusCode, body)
	}
}

func TestConcurrentSubmissionsOverHTTP(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p"})
	const n = 40
	var wg sync.WaitGroup
	statuses := make(chan int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := map[string]interface{}{
				"rendition": "720p", "generation": 1,
				"submissionId": fmt.Sprintf("sub-%d", i),
				"segments":     []domain.Segment{seg(int64(i), 1000, fmt.Sprintf("c-%d", i))},
			}
			b, _ := json.Marshal(payload)
			req, _ := http.NewRequest("POST", c.server.URL+"/v1/streams/s1/segments", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("request error: %v", err)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			statuses <- resp.StatusCode
		}(i)
	}
	wg.Wait()
	close(statuses)
	for s := range statuses {
		if s != http.StatusOK {
			t.Fatalf("expected all 200, got %d", s)
		}
	}

	_, body := c.do("GET", "/v1/streams/s1", nil)
	st := decode[domain.StreamView](t, body)
	if st.Renditions["720p"].HeadSequence != n {
		t.Fatalf("expected contiguous head %d, got %d (gaps or partial commit)", n, st.Renditions["720p"].HeadSequence)
	}
	if st.Revision != n {
		t.Fatalf("expected revision %d, got %d", n, st.Revision)
	}
	if st.Watermark.Sequence != n {
		t.Fatalf("expected watermark %d, got %d", n, st.Watermark.Sequence)
	}
}

func TestResponseRevisionMatchesActualCommit(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p"})
	payload := map[string]interface{}{
		"rendition": "720p", "generation": 1, "submissionId": "s1",
		"segments": []domain.Segment{seg(0, 2000, "a0")},
	}
	_, body := c.do("POST", "/v1/streams/s1/segments", payload)
	submitRes := decode[domain.SubmitResultView](t, body)

	_, body = c.do("GET", "/v1/streams/s1", nil)
	status := decode[domain.StreamView](t, body)

	if submitRes.Revision != status.Revision {
		t.Fatalf("submit revision %d != status revision %d", submitRes.Revision, status.Revision)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	c := newTestClient(t)
	resp, _ := c.do("DELETE", "/v1/streams/s1", nil)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestContextCancellationDoesNotCorrupt(t *testing.T) {
	c := newTestClient(t)
	createStream(t, c, "s1", []string{"720p"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	payload := map[string]interface{}{
		"rendition": "720p", "generation": 1, "submissionId": "s1",
		"segments": []domain.Segment{seg(0, 2000, "a0")},
	}
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, "POST", c.server.URL+"/v1/streams/s1/segments", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}
	_, body := c.do("GET", "/v1/streams/s1", nil)
	st := decode[domain.StreamView](t, body)
	if st.Revision != 0 {
		t.Fatalf("cancelled request must not commit, revision=%d", st.Revision)
	}
}
