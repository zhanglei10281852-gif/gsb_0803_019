package coord

// renditionState holds the buffered segments of one active rendition for the
// stream's current generation. start is where this generation resumes
// publishing (0 for the initial generation, the declared resume point after
// a cutover). head is the last sequence of the gap-free prefix starting at
// start (start-1 when empty); it never crosses a gap and never borrows
// progress from an older generation.
type renditionState struct {
	segments map[int64]Segment
	start    int64
	head     int64
}

func newRenditionState(start int64) *renditionState {
	return &renditionState{segments: make(map[int64]Segment), start: start, head: start - 1}
}

func (r *renditionState) reset(start int64) {
	r.segments = make(map[int64]Segment)
	r.start = start
	r.head = start - 1
}

// submissionRecord pins the fingerprint and the committed result of an
// accepted submission so retries are replayable without re-committing.
type submissionRecord struct {
	fingerprint string
	result      IngestResult
}

// cutoverRecord pins the fingerprint and the committed result of an accepted
// cutover so a retried switch (lost response) replays without re-switching.
type cutoverRecord struct {
	fingerprint string
	result      CutoverResult
}

type streamState struct {
	id          string
	generation  int64
	revision    int64
	renditions  map[string]*renditionState
	submissions map[string]submissionRecord
	cutovers    map[string]cutoverRecord
}

// watermark is the highest sequence every active rendition has completed
// contiguously (from sequence 0) in the current generation; -1 until all
// active renditions have at least sequence 0.
func (s *streamState) watermark() int64 {
	w := int64(-1)
	first := true
	for _, r := range s.renditions {
		if first || r.head < w {
			w = r.head
			first = false
		}
	}
	return w
}

// Store keeps all coordinator state in memory. It is not goroutine-safe on
// its own; callers (Coordinator) must hold the appropriate lock.
type Store struct {
	streams map[string]*streamState
}

func NewStore() *Store {
	return &Store{streams: make(map[string]*streamState)}
}

func (s *Store) get(id string) (*streamState, bool) {
	st, ok := s.streams[id]
	return st, ok
}

func (s *Store) put(st *streamState) {
	s.streams[st.id] = st
}
