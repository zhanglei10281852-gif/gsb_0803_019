// Package service orchestrates domain operations over the storage layer. It is
// the seam between transport (HTTP) and coordination logic: it accepts plain
// request structs, drives the store's locking, and returns snapshots. It holds
// no HTTP or JSON concerns of its own.
package service

import (
	"github.com/gsb/coordinator/internal/domain"
	"github.com/gsb/coordinator/internal/storage"
)

// Coordinator is the application service.
type Coordinator struct {
	store *storage.Store
}

// New builds a Coordinator over the given store.
func New(store *storage.Store) *Coordinator {
	return &Coordinator{store: store}
}

// CreateStreamRequest declares a stream and its active renditions.
type CreateStreamRequest struct {
	StreamID   string
	Renditions []string
}

// CreateStream declares a stream. It is idempotent: re-declaring an existing
// stream with the identical rendition set returns the current snapshot without
// error; a differing declaration is a conflict (KindAlreadyExists).
func (c *Coordinator) CreateStream(req CreateStreamRequest) (domain.Snapshot, bool, error) {
	// Validate the declaration up front so a bad request never creates state.
	if _, err := domain.NewStream(req.StreamID, req.Renditions); err != nil {
		return domain.Snapshot{}, false, err
	}

	st, created, err := c.store.GetOrCreate(req.StreamID, func() (*domain.Stream, error) {
		return domain.NewStream(req.StreamID, req.Renditions)
	})
	if err != nil {
		return domain.Snapshot{}, false, err
	}

	var snap domain.Snapshot
	var conflictErr error
	c.store.With(req.StreamID, func(s *domain.Stream) {
		if !created && !s.SameRenditions(req.Renditions) {
			conflictErr = &domain.Error{
				Kind: domain.KindAlreadyExists,
				Msg:  "stream already exists with a different rendition declaration",
			}
			return
		}
		snap = s.Snapshot()
	})
	if conflictErr != nil {
		return domain.Snapshot{}, false, conflictErr
	}
	_ = st
	return snap, created, nil
}

// SubmitRequest carries one producer submission.
type SubmitRequest struct {
	StreamID     string
	Rendition    string
	Generation   uint64
	SubmissionID string
	Segments     []domain.Segment
}

// SubmitResponse pairs the submit result with a fresh snapshot taken under the
// same lock, so the revision it reports is exactly the committed one.
type SubmitResponse struct {
	Result   domain.SubmitResult
	Snapshot domain.Snapshot
}

// Submit applies a batch to an existing stream.
func (c *Coordinator) Submit(req SubmitRequest) (SubmitResponse, error) {
	var resp SubmitResponse
	var opErr error
	found := c.store.With(req.StreamID, func(s *domain.Stream) {
		res, err := s.Submit(req.Rendition, req.Generation, req.SubmissionID, req.Segments)
		if err != nil {
			opErr = err
			return
		}
		resp.Result = res
		resp.Snapshot = s.Snapshot()
	})
	if !found {
		return SubmitResponse{}, &domain.Error{Kind: domain.KindNotFound, Msg: "stream not found"}
	}
	if opErr != nil {
		return SubmitResponse{}, opErr
	}
	return resp, nil
}

// Status returns the current snapshot of a stream.
func (c *Coordinator) Status(streamID string) (domain.Snapshot, error) {
	var snap domain.Snapshot
	found := c.store.With(streamID, func(s *domain.Stream) {
		snap = s.Snapshot()
	})
	if !found {
		return domain.Snapshot{}, &domain.Error{Kind: domain.KindNotFound, Msg: "stream not found"}
	}
	return snap, nil
}

// CutoverRequest carries a lossless active/standby switch.
type CutoverRequest struct {
	StreamID         string
	CutoverID        string
	ExpectedRevision uint64
	Generation       uint64
	Renditions       []string
	SplicePoints     map[string]uint64
}

// CutoverResponse pairs the cutover result with a snapshot taken under the same
// lock, so the reported revision matches committed state.
type CutoverResponse struct {
	Result   domain.CutoverResult
	Snapshot domain.Snapshot
}

// Cutover performs an atomic generation switch on an existing stream.
func (c *Coordinator) Cutover(req CutoverRequest) (CutoverResponse, error) {
	var resp CutoverResponse
	var opErr error
	found := c.store.With(req.StreamID, func(s *domain.Stream) {
		res, err := s.Cutover(domain.CutoverSpec{
			CutoverID:        req.CutoverID,
			ExpectedRevision: req.ExpectedRevision,
			Generation:       req.Generation,
			Renditions:       req.Renditions,
			SplicePoints:     req.SplicePoints,
		})
		if err != nil {
			opErr = err
			return
		}
		resp.Result = res
		resp.Snapshot = s.Snapshot()
	})
	if !found {
		return CutoverResponse{}, &domain.Error{Kind: domain.KindNotFound, Msg: "stream not found"}
	}
	if opErr != nil {
		return CutoverResponse{}, opErr
	}
	return resp, nil
}
