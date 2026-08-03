package coord

// renditionState holds the buffered segments of one active rendition for the
// stream's current generation. head is the last sequence of the gap-free
// prefix starting at sequence 0 (-1 when empty); it never crosses a gap.
type renditionState struct {
	segments map[int64]Segment
	head     int64
}

func newRenditionState() *renditionState {
	return &renditionState{segments: make(map[int64]Segment), head: -1}
}

func (r *renditionState) reset() {
	r.segments = make(map[int64]Segment)
	r.head = -1
}

// submissionRecord pins the fingerprint and the committed result of an
// accepted submission so retries are replayable without re-committing.
type submissionRecord struct {
	fingerprint string
	result      IngestResult
}

type streamState struct {
	id          string
	generation  int64
	revision    int64
	renditions  map[string]*renditionState
	submissions map[string]submissionRecord
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
