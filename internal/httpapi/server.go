// Package httpapi exposes the coordinator as a JSON/HTTP service. It owns the
// wire contract only: decoding requests, encoding responses, and mapping domain
// error kinds onto status codes. All coordination logic lives behind the
// service layer.
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gsb/coordinator/internal/domain"
	"github.com/gsb/coordinator/internal/service"
)

// Server adapts a service.Coordinator to HTTP.
type Server struct {
	coord *service.Coordinator
	mux   *http.ServeMux
}

// New builds a Server with routes registered.
func New(coord *service.Coordinator) *Server {
	s := &Server{coord: coord, mux: http.NewServeMux()}
	s.routes()
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("POST /v1/streams", s.handleCreateStream)
	s.mux.HandleFunc("POST /v1/streams/{streamId}/segments", s.handleSubmit)
	s.mux.HandleFunc("POST /v1/streams/{streamId}/cutover", s.handleCutover)
	s.mux.HandleFunc("GET /v1/streams/{streamId}", s.handleStatus)
}

// --- wire types ---

type createStreamBody struct {
	StreamID   string   `json:"streamId"`
	Renditions []string `json:"renditions"`
}

type segmentBody struct {
	Sequence   uint64 `json:"sequence"`
	DurationMs uint64 `json:"durationMs"`
	SHA256     string `json:"sha256"`
}

type submitBody struct {
	Rendition    string        `json:"rendition"`
	Generation   uint64        `json:"generation"`
	SubmissionID string        `json:"submissionId"`
	Segments     []segmentBody `json:"segments"`
}

type cutoverBody struct {
	CutoverID        string            `json:"cutoverId"`
	ExpectedRevision uint64            `json:"expectedRevision"`
	Generation       uint64            `json:"generation"`
	Renditions       []string          `json:"renditions"`
	SplicePoints     map[string]uint64 `json:"splicePoints"`
}

type renditionStatusView struct {
	Generation   *uint64  `json:"generation"`
	SplicePoint  *uint64  `json:"splicePoint"`
	Head         *uint64  `json:"head"`
	SegmentCount int      `json:"segmentCount"`
	Buffered     []uint64 `json:"buffered"`
}

type statusView struct {
	StreamID          string                         `json:"streamId"`
	Revision          uint64                         `json:"revision"`
	ActiveRenditions  []string                       `json:"activeRenditions"`
	CurrentGeneration *uint64                        `json:"currentGeneration"`
	Watermark         *uint64                        `json:"watermark"`
	Renditions        map[string]renditionStatusView `json:"renditions"`
}

type submitView struct {
	Applied bool       `json:"applied"`
	Status  statusView `json:"status"`
}

type errorView struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// --- handlers ---

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleCreateStream(w http.ResponseWriter, r *http.Request) {
	var body createStreamBody
	if !decode(w, r, &body) {
		return
	}
	snap, created, err := s.coord.CreateStream(service.CreateStreamRequest{
		StreamID:   body.StreamID,
		Renditions: body.Renditions,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, toStatusView(snap))
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var body submitBody
	if !decode(w, r, &body) {
		return
	}
	segs := make([]domain.Segment, len(body.Segments))
	for i, sb := range body.Segments {
		segs[i] = domain.Segment{Sequence: sb.Sequence, DurationMs: sb.DurationMs, SHA256: sb.SHA256}
	}
	resp, err := s.coord.Submit(service.SubmitRequest{
		StreamID:     r.PathValue("streamId"),
		Rendition:    body.Rendition,
		Generation:   body.Generation,
		SubmissionID: body.SubmissionID,
		Segments:     segs,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	// A fresh submission returns 201; an idempotent replay returns 200.
	code := http.StatusOK
	if resp.Result.Applied {
		code = http.StatusCreated
	}
	writeJSON(w, code, submitView{Applied: resp.Result.Applied, Status: toStatusView(resp.Snapshot)})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	snap, err := s.coord.Status(r.PathValue("streamId"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toStatusView(snap))
}

func (s *Server) handleCutover(w http.ResponseWriter, r *http.Request) {
	var body cutoverBody
	if !decode(w, r, &body) {
		return
	}
	resp, err := s.coord.Cutover(service.CutoverRequest{
		StreamID:         r.PathValue("streamId"),
		CutoverID:        body.CutoverID,
		ExpectedRevision: body.ExpectedRevision,
		Generation:       body.Generation,
		Renditions:       body.Renditions,
		SplicePoints:     body.SplicePoints,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	// A fresh cutover returns 201; an idempotent retry returns 200.
	code := http.StatusOK
	if resp.Result.Applied {
		code = http.StatusCreated
	}
	writeJSON(w, code, submitView{Applied: resp.Result.Applied, Status: toStatusView(resp.Snapshot)})
}

// --- helpers ---

func toStatusView(snap domain.Snapshot) statusView {
	rends := make(map[string]renditionStatusView, len(snap.Renditions))
	for name, rs := range snap.Renditions {
		buffered := rs.Buffered
		if buffered == nil {
			buffered = []uint64{}
		}
		rends[name] = renditionStatusView{
			Generation:   rs.Generation,
			SplicePoint:  rs.SplicePoint,
			Head:         rs.Head,
			SegmentCount: rs.SegmentCount,
			Buffered:     buffered,
		}
	}
	active := snap.ActiveRenditions
	if active == nil {
		active = []string{}
	}
	return statusView{
		StreamID:          snap.StreamID,
		Revision:          snap.Revision,
		ActiveRenditions:  active,
		CurrentGeneration: snap.CurrentGeneration,
		Watermark:         snap.Watermark,
		Renditions:        rends,
	}
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, errorView{Error: "invalid JSON body: " + err.Error(), Code: "validation"})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeErr(w http.ResponseWriter, err error) {
	kind := domain.KindOf(err)
	var code int
	var label string
	switch kind {
	case domain.KindValidation:
		code, label = http.StatusBadRequest, "validation"
	case domain.KindConflict:
		code, label = http.StatusConflict, "conflict"
	case domain.KindAlreadyExists:
		code, label = http.StatusConflict, "already_exists"
	case domain.KindFenced:
		// A superseded generation is a stable conflict with the linearized
		// cutover; it is rejected without side effects.
		code, label = http.StatusConflict, "fenced"
	case domain.KindNotFound:
		code, label = http.StatusNotFound, "not_found"
	default:
		code, label = http.StatusInternalServerError, "internal"
	}
	msg := err.Error()
	var de *domain.Error
	if errors.As(err, &de) {
		msg = de.Msg
	}
	writeJSON(w, code, errorView{Error: msg, Code: label})
}
