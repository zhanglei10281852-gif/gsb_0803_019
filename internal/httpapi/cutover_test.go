package httpapi_test

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"livecoord/internal/coord"
)

func cutoverBody(id string, expectedRev, gen int64, resumes ...coord.RenditionResume) coord.CutoverRequest {
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

func TestCutoverFlowOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a", "b")
	mustIngest(t, s.URL, "live-1", batch("sub-1", "a", 0, 0, 1))
	mustIngest(t, s.URL, "live-1", batch("sub-2", "b", 0, 0)) // revision 3, watermark 0

	code, data := call(t, http.MethodPost, s.URL+"/v1/streams/live-1/cutover",
		cutoverBody("co-1", 3, 5, resume("a", 100), resume("b", 98)))
	if code != http.StatusOK {
		t.Fatalf("cutover = %d %s, want 200", code, data)
	}
	res := decode[coord.CutoverResult](t, data)
	if res.Revision != 4 || res.Generation != 5 || res.Watermark != 97 || res.Replayed {
		t.Fatalf("cutover result = %+v", res)
	}
	if res.Renditions["a"].Head != 99 || res.Renditions["b"].Head != 97 {
		t.Fatalf("renditions = %+v, want heads 99/97", res.Renditions)
	}

	st := mustStatus(t, s.URL, "live-1")
	if st.Revision != 4 || st.Generation != 5 || st.Watermark != 97 {
		t.Fatalf("status = %+v", st)
	}

	// Partial new-primary readiness: watermark only advances where every
	// rendition is contiguous past its resume point.
	res2 := mustIngest(t, s.URL, "live-1", batch("sub-10", "a", 5, 100, 101))
	if res2.Watermark != 97 {
		t.Fatalf("watermark = %d, want 97 (b still at resume point)", res2.Watermark)
	}
	res2 = mustIngest(t, s.URL, "live-1", batch("sub-11", "b", 5, 98, 99, 100, 101))
	if res2.Watermark != 101 {
		t.Fatalf("watermark = %d, want 101", res2.Watermark)
	}
}

func TestCutoverReplayAfterLostResponseOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a")
	body := cutoverBody("co-1", 1, 5, resume("a", 100))

	code, data := call(t, http.MethodPost, s.URL+"/v1/streams/live-1/cutover", body)
	if code != http.StatusOK {
		t.Fatalf("cutover = %d %s, want 200", code, data)
	}
	first := decode[coord.CutoverResult](t, data)

	code, data = call(t, http.MethodPost, s.URL+"/v1/streams/live-1/cutover", body)
	if code != http.StatusOK {
		t.Fatalf("retry = %d %s, want 200", code, data)
	}
	second := decode[coord.CutoverResult](t, data)
	if !second.Replayed || second.Revision != first.Revision || second.Generation != first.Generation {
		t.Fatalf("retry = %+v, want original result %+v replayed", second, first)
	}

	st := mustStatus(t, s.URL, "live-1")
	if st.Revision != first.Revision || st.Generation != 5 {
		t.Fatalf("status = %+v, want switched exactly once", st)
	}
}

func TestCutoverConflictsOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a")
	mustIngest(t, s.URL, "live-1", batch("sub-1", "a", 0, 0)) // revision 2
	url := s.URL + "/v1/streams/live-1/cutover"

	conflicts := []struct {
		name string
		body coord.CutoverRequest
		code string
	}{
		{"stale expectedRevision", cutoverBody("co-1", 1, 1, resume("a", 100)), coord.CodeRevisionConflict},
		{"generation regression", cutoverBody("co-2", 2, 0, resume("a", 100)), coord.CodeStaleGeneration},
	}
	for _, tc := range conflicts {
		t.Run(tc.name, func(t *testing.T) {
			code, data := call(t, http.MethodPost, url, tc.body)
			if code != http.StatusConflict {
				t.Fatalf("%s = %d %s, want 409", tc.name, code, data)
			}
			if got := decode[errResp](t, data).Error.Code; got != tc.code {
				t.Fatalf("%s error code = %q, want %q", tc.name, got, tc.code)
			}
			// Deterministic: the same rejected request conflicts again.
			code, _ = call(t, http.MethodPost, url, tc.body)
			if code != http.StatusConflict {
				t.Fatalf("%s repeat = %d, want 409", tc.name, code)
			}
		})
	}

	invalid := []struct {
		name string
		body coord.CutoverRequest
	}{
		{"empty rendition set", cutoverBody("co-3", 2, 1)},
		{"duplicate renditions", cutoverBody("co-4", 2, 1, resume("a", 1), resume("a", 2))},
		{"negative startSequence", cutoverBody("co-5", 2, 1, resume("a", -5))},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			code, data := call(t, http.MethodPost, url, tc.body)
			if code != http.StatusBadRequest {
				t.Fatalf("%s = %d %s, want 400", tc.name, code, data)
			}
			if got := decode[errResp](t, data).Error.Code; got != coord.CodeInvalidArgument {
				t.Fatalf("%s error code = %q, want %q", tc.name, got, coord.CodeInvalidArgument)
			}
		})
	}

	// None of the rejected cutovers changed the stream.
	st := mustStatus(t, s.URL, "live-1")
	if st.Revision != 2 || st.Generation != 0 {
		t.Fatalf("status = %+v, want untouched (revision 2 generation 0)", st)
	}

	// Same cutoverId with drifted content after a successful switch.
	mustCreateStream(t, s.URL, "live-2", "a")
	code, data := call(t, http.MethodPost, s.URL+"/v1/streams/live-2/cutover",
		cutoverBody("co-9", 1, 5, resume("a", 100)))
	if code != http.StatusOK {
		t.Fatalf("cutover = %d %s", code, data)
	}
	code, data = call(t, http.MethodPost, s.URL+"/v1/streams/live-2/cutover",
		cutoverBody("co-9", 1, 5, resume("a", 101)))
	if code != http.StatusConflict {
		t.Fatalf("drifted cutoverId = %d %s, want 409", code, data)
	}
	if got := decode[errResp](t, data).Error.Code; got != coord.CodeCutoverConflict {
		t.Fatalf("error code = %q, want %q", got, coord.CodeCutoverConflict)
	}

	// Unknown stream.
	code, data = call(t, http.MethodPost, s.URL+"/v1/streams/ghost/cutover",
		cutoverBody("co-1", 1, 1, resume("a", 1)))
	if code != http.StatusNotFound || decode[errResp](t, data).Error.Code != coord.CodeNotFound {
		t.Fatalf("unknown stream = %d %s, want 404 not_found", code, data)
	}
}

func TestLateOldGenerationFencedOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a")
	mustIngest(t, s.URL, "live-1", batch("sub-1", "a", 0, 0, 1)) // revision 2

	code, data := call(t, http.MethodPost, s.URL+"/v1/streams/live-1/cutover",
		cutoverBody("co-1", 2, 3, resume("a", 100)))
	if code != http.StatusOK {
		t.Fatalf("cutover = %d %s", code, data)
	}

	// A late write on the old generation is fenced and changes nothing.
	code, data = call(t, http.MethodPost, s.URL+"/v1/streams/live-1/ingest", batch("sub-late", "a", 0, 2))
	if code != http.StatusConflict {
		t.Fatalf("late write = %d, want 409", code)
	}
	if got := decode[errResp](t, data).Error.Code; got != coord.CodeStaleGeneration {
		t.Fatalf("error code = %q, want %q", got, coord.CodeStaleGeneration)
	}
	st := mustStatus(t, s.URL, "live-1")
	if st.Revision != 3 || st.Watermark != 99 || st.Renditions["a"].Buffered != 0 {
		t.Fatalf("status after fenced write = %+v, want untouched", st)
	}

	// A retry of a pre-cutover committed submission still replays without
	// side effects.
	code, data = call(t, http.MethodPost, s.URL+"/v1/streams/live-1/ingest", batch("sub-1", "a", 0, 0, 1))
	if code != http.StatusOK {
		t.Fatalf("old replay = %d %s, want 200", code, data)
	}
	res := decode[coord.IngestResult](t, data)
	if !res.Replayed || res.Revision != 2 {
		t.Fatalf("old replay = %+v, want revision 2 replayed", res)
	}
	st = mustStatus(t, s.URL, "live-1")
	if st.Revision != 3 || st.Watermark != 99 {
		t.Fatalf("status after old replay = %+v, want revision 3 watermark 99", st)
	}
}

func TestConcurrentCutoversOverHTTP(t *testing.T) {
	s := newServer(t)
	mustCreateStream(t, s.URL, "live-1", "a")

	const n = 8
	statuses := make([]int, n)
	codes := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			code, data := call(t, http.MethodPost, s.URL+"/v1/streams/live-1/cutover",
				cutoverBody(fmt.Sprintf("co-%d", i), 1, int64(10+i), resume("a", 100)))
			statuses[i] = code
			if code == http.StatusOK {
				codes[i] = "ok"
				return
			}
			codes[i] = decode[errResp](t, data).Error.Code
		}(i)
	}
	wg.Wait()

	wins := 0
	for i, code := range codes {
		switch code {
		case "ok":
			wins++
		case coord.CodeRevisionConflict:
			// expected loser outcome
		default:
			t.Fatalf("goroutine %d: status %d code %q", i, statuses[i], code)
		}
	}
	if wins != 1 {
		t.Fatalf("wins = %d, want exactly 1", wins)
	}
	st := mustStatus(t, s.URL, "live-1")
	if st.Revision != 2 {
		t.Fatalf("status revision = %d, want exactly one switch", st.Revision)
	}
}
