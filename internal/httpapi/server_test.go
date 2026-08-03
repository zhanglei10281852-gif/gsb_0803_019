package httpapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"

	"livecoord/internal/coord"
	"livecoord/internal/httpapi"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(httpapi.NewServer(coord.NewCoordinator(coord.NewStore())))
	t.Cleanup(s.Close)
	return s
}

func call(t *testing.T, method, url string, body any) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	return callRaw(t, method, url, rdr, "application/json")
}

func callRaw(t *testing.T, method, url string, body io.Reader, contentType string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, data
}

func decode[T any](t *testing.T, data []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("decode %s: %v", data, err)
	}
	return v
}

type errResp struct {
	Error coord.Error `json:"error"`
}

func seg(seq int64) coord.Segment {
	return coord.Segment{Sequence: seq, DurationMs: 2000, SHA256: fmt.Sprintf("%064x", seq+1)}
}

func batch(subID, rendition string, gen int64, seqs ...int64) coord.IngestRequest {
	segs := make([]coord.Segment, len(seqs))
	for i, s := range seqs {
		segs[i] = seg(s)
	}
	return coord.IngestRequest{SubmissionID: subID, Rendition: rendition, Generation: gen, Segments: segs}
}

func mustCreateStream(t *testing.T, base, id string, rends ...string) coord.Status {
	t.Helper()
	code, data := call(t, http.MethodPost, base+"/v1/streams", map[string]any{
		"streamId":   id,
		"renditions": rends,
	})
	if code != http.StatusCreated {
		t.Fatalf("create stream = %d %s, want 201", code, data)
	}
	return decode[coord.Status](t, data)
}

func mustIngest(t *testing.T, base, stream string, req coord.IngestRequest) coord.IngestResult {
	t.Helper()
	code, data := call(t, http.MethodPost, base+"/v1/streams/"+stream+"/ingest", req)
	if code != http.StatusOK {
		t.Fatalf("ingest = %d %s, want 200", code, data)
	}
	return decode[coord.IngestResult](t, data)
}

func mustStatus(t *testing.T, base, stream string) coord.Status {
	t.Helper()
	code, data := call(t, http.MethodGet, base+"/v1/streams/"+stream+"/status", nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d %s, want 200", code, data)
	}
	return decode[coord.Status](t, data)
}

func TestHealthCheck(t *testing.T) {
	s := newServer(t)
	code, data := call(t, http.MethodGet, s.URL+"/healthz", nil)
	if code != http.StatusOK {
		t.Fatalf("health = %d, want 200", code)
	}
	body := decode[map[string]string](t, data)
	if body["status"] != "ok" {
		t.Fatalf("health body = %v", body)
	}
}

func TestFullPublishFlow(t *testing.T) {
	s := newServer(t)

	st := mustCreateStream(t, s.URL, "live-1", "360p", "720p")
	if st.Revision != 1 || st.Watermark != -1 || st.Renditions["720p"].Head != -1 {
		t.Fatalf("fresh stream = %+v", st)
	}

	res := mustIngest(t, s.URL, "live-1", batch("sub-100", "720p", 0, 1, 2))
	if res.Head != -1 || res.Watermark != -1 || res.Revision != 2 {
		t.Fatalf("out-of-order ingest = %+v, want head -1 watermark -1 revision 2", res)
	}

	res = mustIngest(t, s.URL, "live-1", batch("sub-101", "720p", 0, 0))
	if res.Head != 2 {
		t.Fatalf("head = %d, want 2 after gap fill", res.Head)
	}
	if res.Watermark != -1 {
		t.Fatalf("watermark = %d, want -1 (360p not started)", res.Watermark)
	}

	res = mustIngest(t, s.URL, "live-1", batch("sub-102", "360p", 0, 0, 1))
	if res.Watermark != 1 {
		t.Fatalf("watermark = %d, want 1 (min of heads 2 and 1)", res.Watermark)
	}

	st = mustStatus(t, s.URL, "live-1")
	if st.Revision != 4 || st.Watermark != 1 {
		t.Fatalf("status = %+v, want revision 4 watermark 1", st)
	}
	if st.Renditions["720p"].Head != 2 || st.Renditions["360p"].Head != 1 {
		t.Fatalf("heads = %+v", st.Renditions)
	}
}

func TestIdempotentRetryOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a")

	body := batch("sub-1", "a", 0, 0, 1, 2)
	first := mustIngest(t, s.URL, "live-1", body)

	code, data := call(t, http.MethodPost, s.URL+"/v1/streams/live-1/ingest", body)
	if code != http.StatusOK {
		t.Fatalf("retry = %d %s, want 200", code, data)
	}
	second := decode[coord.IngestResult](t, data)
	if !second.Replayed {
		t.Fatal("retry must report replayed=true")
	}
	if second.Revision != first.Revision || second.Head != first.Head || second.Applied != first.Applied {
		t.Fatalf("retry = %+v, want identical committed result %+v", second, first)
	}

	st := mustStatus(t, s.URL, "live-1")
	if st.Revision != first.Revision {
		t.Fatalf("status revision = %d, want %d (retry must not advance)", st.Revision, first.Revision)
	}
}

func TestSubmissionConflictOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a")
	mustIngest(t, s.URL, "live-1", batch("sub-1", "a", 0, 0, 1))

	code, data := call(t, http.MethodPost, s.URL+"/v1/streams/live-1/ingest", batch("sub-1", "a", 0, 0, 2))
	if code != http.StatusConflict {
		t.Fatalf("conflict = %d, want 409", code)
	}
	if got := decode[errResp](t, data).Error.Code; got != coord.CodeSubmissionConflict {
		t.Fatalf("error code = %q, want %q", got, coord.CodeSubmissionConflict)
	}

	st := mustStatus(t, s.URL, "live-1")
	if st.Revision != 2 {
		t.Fatalf("status revision = %d, want 2 (conflict must not advance)", st.Revision)
	}
}

func TestStaleGenerationOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a")
	mustIngest(t, s.URL, "live-1", batch("sub-1", "a", 1, 0))

	code, data := call(t, http.MethodPost, s.URL+"/v1/streams/live-1/ingest", batch("sub-2", "a", 0, 0))
	if code != http.StatusConflict {
		t.Fatalf("stale generation = %d, want 409", code)
	}
	if got := decode[errResp](t, data).Error.Code; got != coord.CodeStaleGeneration {
		t.Fatalf("error code = %q, want %q", got, coord.CodeStaleGeneration)
	}
}

func TestNotFoundOverHTTP(t *testing.T) {
	s := newServer(t)

	code, data := call(t, http.MethodGet, s.URL+"/v1/streams/ghost/status", nil)
	if code != http.StatusNotFound || decode[errResp](t, data).Error.Code != coord.CodeNotFound {
		t.Fatalf("status of unknown stream = %d %s", code, data)
	}

	code, data = call(t, http.MethodPost, s.URL+"/v1/streams/ghost/ingest", batch("sub-1", "a", 0, 0))
	if code != http.StatusNotFound || decode[errResp](t, data).Error.Code != coord.CodeNotFound {
		t.Fatalf("ingest to unknown stream = %d %s", code, data)
	}
}

func TestValidationOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a")
	ingestURL := s.URL + "/v1/streams/live-1/ingest"

	cases := []struct {
		name string
		body any
	}{
		{"unknown field", map[string]any{"submissionId": "x", "rendition": "a", "generation": 0, "segments": []any{seg(0)}, "bogus": 1}},
		{"empty segments", map[string]any{"submissionId": "x", "rendition": "a", "generation": 0, "segments": []any{}}},
		{"missing submissionId", map[string]any{"rendition": "a", "generation": 0, "segments": []any{seg(0)}}},
		{"bad sha256", coord.IngestRequest{SubmissionID: "x", Rendition: "a", Generation: 0, Segments: []coord.Segment{{Sequence: 0, DurationMs: 2000, SHA256: "zz"}}}},
		{"negative sequence", coord.IngestRequest{SubmissionID: "x", Rendition: "a", Generation: 0, Segments: []coord.Segment{{Sequence: -1, DurationMs: 2000, SHA256: fmt.Sprintf("%064x", 1)}}}},
		{"zero duration", coord.IngestRequest{SubmissionID: "x", Rendition: "a", Generation: 0, Segments: []coord.Segment{{Sequence: 0, DurationMs: 0, SHA256: fmt.Sprintf("%064x", 1)}}}},
		{"inactive rendition", batch("x", "ghost", 0, 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, data := call(t, http.MethodPost, ingestURL, tc.body)
			if code != http.StatusBadRequest {
				t.Fatalf("%s = %d %s, want 400", tc.name, code, data)
			}
			if got := decode[errResp](t, data).Error.Code; got != coord.CodeInvalidArgument {
				t.Fatalf("%s error code = %q, want %q", tc.name, got, coord.CodeInvalidArgument)
			}
		})
	}

	// No failed request above may have left partial state.
	st := mustStatus(t, s.URL, "live-1")
	if st.Revision != 1 || st.Renditions["a"].Buffered != 0 {
		t.Fatalf("status after failed requests = %+v, want untouched", st)
	}
}

func TestMalformedRequestsOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a")
	ingestURL := s.URL + "/v1/streams/live-1/ingest"

	code, _ := callRaw(t, http.MethodPost, ingestURL, bytes.NewReader([]byte(`{"submissionId":"x"} {"more":true}`)), "application/json")
	if code != http.StatusBadRequest {
		t.Fatalf("two JSON values = %d, want 400", code)
	}

	code, _ = callRaw(t, http.MethodPost, ingestURL, bytes.NewReader([]byte(`not json`)), "application/json")
	if code != http.StatusBadRequest {
		t.Fatalf("garbage body = %d, want 400", code)
	}

	code, data := callRaw(t, http.MethodPost, ingestURL, bytes.NewReader([]byte(`{}`)), "text/plain")
	if code != http.StatusUnsupportedMediaType {
		t.Fatalf("wrong content type = %d %s, want 415", code, data)
	}

	code, _ = call(t, http.MethodGet, ingestURL, nil)
	if code != http.StatusMethodNotAllowed {
		t.Fatalf("GET on ingest = %d, want 405", code)
	}
}

func TestConcurrentIngestOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a")

	const n = 32
	revs := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			code, data := call(t, http.MethodPost, s.URL+"/v1/streams/live-1/ingest",
				batch(fmt.Sprintf("sub-%d", i), "a", 0, int64(i)))
			if code != http.StatusOK {
				t.Errorf("ingest %d = %d %s, want 200", i, code, data)
				return
			}
			revs[i] = decode[coord.IngestResult](t, data).Revision
		}(i)
	}
	wg.Wait()

	sort.Slice(revs, func(i, j int) bool { return revs[i] < revs[j] })
	for i, r := range revs {
		if want := int64(i + 2); r != want {
			t.Fatalf("sorted revisions[%d] = %d, want %d", i, r, want)
		}
	}

	st := mustStatus(t, s.URL, "live-1")
	if st.Revision != int64(n+1) || st.Renditions["a"].Head != int64(n-1) {
		t.Fatalf("final status = %+v, want revision %d head %d", st, n+1, n-1)
	}
}

func TestConcurrentDuplicateSubmissionOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a")

	const n = 16
	results := make([]coord.IngestResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			code, data := call(t, http.MethodPost, s.URL+"/v1/streams/live-1/ingest", batch("sub-1", "a", 0, 0, 1))
			if code != http.StatusOK {
				t.Errorf("ingest = %d %s, want 200", code, data)
				return
			}
			results[i] = decode[coord.IngestResult](t, data)
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
		t.Fatalf("commits = %d, want exactly 1", commits)
	}
}
