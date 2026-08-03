// Package httpapi exposes the segment publish coordinator over a JSON HTTP
// API. It only translates between HTTP/JSON and domain calls; all
// coordination rules live in the coord package.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"livecoord/internal/coord"
)

const maxBodyBytes = 4 << 20

const (
	codeUnsupportedMediaType = "unsupported_media_type"
	codePayloadTooLarge      = "payload_too_large"
	codeInternal             = "internal"
)

// Server routes:
//
//	POST /v1/streams                    create a stream, declaring active renditions
//	POST /v1/streams/{streamId}/ingest  commit a batch of segments (idempotent by submissionId)
//	GET  /v1/streams/{streamId}/status  revision, per-rendition heads, publish watermark
//	GET  /healthz                       liveness probe
type Server struct {
	coord *coord.Coordinator
	mux   *http.ServeMux
}

func NewServer(c *coord.Coordinator) *Server {
	s := &Server{coord: c}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/streams", s.handleCreateStream)
	mux.HandleFunc("POST /v1/streams/{streamId}/ingest", s.handleIngest)
	mux.HandleFunc("GET /v1/streams/{streamId}/status", s.handleStatus)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

type errorResponse struct {
	Error coord.Error `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	ce := &coord.Error{}
	if !errors.As(err, &ce) {
		ce = &coord.Error{Code: codeInternal, Message: "internal error"}
	}
	writeJSON(w, statusForCode(ce.Code), errorResponse{Error: *ce})
}

func statusForCode(code string) int {
	switch code {
	case coord.CodeInvalidArgument:
		return http.StatusBadRequest
	case coord.CodeNotFound:
		return http.StatusNotFound
	case coord.CodeAlreadyExists,
		coord.CodeSubmissionConflict,
		coord.CodeSegmentConflict,
		coord.CodeStaleGeneration:
		return http.StatusConflict
	case codeUnsupportedMediaType:
		return http.StatusUnsupportedMediaType
	case codePayloadTooLarge:
		return http.StatusRequestEntityTooLarge
	default:
		return http.StatusInternalServerError
	}
}

// decodeJSON strictly decodes a single JSON object body into dst.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil || mt != "application/json" {
			return &coord.Error{Code: codeUnsupportedMediaType, Message: "Content-Type must be application/json"}
		}
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return &coord.Error{Code: codePayloadTooLarge, Message: fmt.Sprintf("body exceeds %d bytes", maxBodyBytes)}
		}
		return &coord.Error{Code: coord.CodeInvalidArgument, Message: "invalid JSON body: " + err.Error()}
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return &coord.Error{Code: coord.CodeInvalidArgument, Message: "body must contain a single JSON value"}
	}
	return nil
}

type createStreamRequest struct {
	StreamID   string   `json:"streamId"`
	Renditions []string `json:"renditions"`
}

func (s *Server) handleCreateStream(w http.ResponseWriter, r *http.Request) {
	var req createStreamRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	st, err := s.coord.CreateStream(req.StreamID, req.Renditions)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, st)
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var req coord.IngestRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	res, err := s.coord.Ingest(r.PathValue("streamId"), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.coord.Status(r.PathValue("streamId"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
