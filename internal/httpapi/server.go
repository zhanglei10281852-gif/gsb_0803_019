package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"livecoord/internal/domain"
)

type Server struct {
	coord   *domain.Coordinator
	mux     *http.ServeMux
	handler http.Handler
}

func NewServer(coord *domain.Coordinator) *Server {
	s := &Server{coord: coord, mux: http.NewServeMux()}
	s.routes()
	s.handler = logging(s.mux)
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("POST /v1/streams", s.handleCreateStream)
	s.mux.HandleFunc("POST /v1/streams/{streamId}/segments", s.handleSubmitSegments)
	s.mux.HandleFunc("POST /v1/streams/{streamId}/cutover", s.handleCutover)
	s.mux.HandleFunc("GET /v1/streams/{streamId}", s.handleGetStream)
}

func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.coord.Health(r.Context()))
}

type createStreamRequest struct {
	StreamID         string   `json:"streamId"`
	ActiveRenditions []string `json:"activeRenditions"`
}

func (s *Server) handleCreateStream(w http.ResponseWriter, r *http.Request) {
	var req createStreamRequest
	if err := decodeJSON(r, &req); err != nil {
		writeDomainError(w, err)
		return
	}
	view, err := s.coord.CreateStream(r.Context(), domain.CreateStreamInput{
		StreamID:         req.StreamID,
		ActiveRenditions: req.ActiveRenditions,
	})
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, view)
}

type submitSegmentsRequest struct {
	Rendition    string           `json:"rendition"`
	Generation   int64            `json:"generation"`
	SubmissionID string           `json:"submissionId"`
	Segments     []domain.Segment `json:"segments"`
}

func (s *Server) handleSubmitSegments(w http.ResponseWriter, r *http.Request) {
	streamID := r.PathValue("streamId")
	var req submitSegmentsRequest
	if err := decodeJSON(r, &req); err != nil {
		writeDomainError(w, err)
		return
	}
	view, err := s.coord.SubmitSegments(r.Context(), domain.SubmitSegmentsInput{
		StreamID:     streamID,
		Rendition:    req.Rendition,
		Generation:   req.Generation,
		SubmissionID: req.SubmissionID,
		Segments:     req.Segments,
	})
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) handleGetStream(w http.ResponseWriter, r *http.Request) {
	streamID := r.PathValue("streamId")
	view, err := s.coord.GetStatus(r.Context(), streamID)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

type cutoverRequest struct {
	Generation       int64            `json:"generation"`
	CutoverID        string           `json:"cutoverId"`
	ExpectedRevision int64            `json:"expectedRevision"`
	Renditions       map[string]int64 `json:"renditions"`
}

func (s *Server) handleCutover(w http.ResponseWriter, r *http.Request) {
	streamID := r.PathValue("streamId")
	var req cutoverRequest
	if err := decodeJSON(r, &req); err != nil {
		writeDomainError(w, err)
		return
	}
	view, err := s.coord.Cutover(r.Context(), domain.CutoverInput{
		StreamID:         streamID,
		Generation:       req.Generation,
		CutoverID:        req.CutoverID,
		ExpectedRevision: req.ExpectedRevision,
		Renditions:       req.Renditions,
	})
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func decodeJSON(r *http.Request, dst interface{}) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return domain.NewError(domain.ErrCodeInvalidRequest, "invalid JSON: "+err.Error())
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func writeDomainError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	var de *domain.Error
	if errors.As(err, &de) {
		writeJSON(w, statusForCode(de.Code), errorBody{Error: de.Code, Message: de.Message})
		return
	}
	switch {
	case errors.Is(err, context.Canceled):
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "CANCELED", Message: err.Error()})
	case errors.Is(err, context.DeadlineExceeded):
		writeJSON(w, http.StatusGatewayTimeout, errorBody{Error: "TIMEOUT", Message: err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: domain.ErrCodeInternal, Message: err.Error()})
	}
}

func statusForCode(code string) int {
	switch code {
	case domain.ErrCodeStreamNotFound:
		return http.StatusNotFound
	case domain.ErrCodeStreamExists,
		domain.ErrCodeConflict,
		domain.ErrCodeStaleGeneration,
		domain.ErrCodeRevisionMismatch,
		domain.ErrCodeCutoverConflict,
		domain.ErrCodeCutoverRequired,
		domain.ErrCodeSegmentOutOfRange:
		return http.StatusConflict
	case domain.ErrCodeInvalidRequest:
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.Printf("%s %s -> %d (%s)", r.Method, r.URL.Path, rec.status, time.Since(start))
	})
}
