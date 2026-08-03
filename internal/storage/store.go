// Package storage provides in-memory persistence for streams. It owns
// concurrency control: each stream is guarded by its own mutex, so all
// operations on a single stream are serialized. Because a mutation and the
// snapshot returned to the caller run under the same lock, the revision a
// client observes always matches committed state, and concurrent requests are
// equivalent to some serial order.
package storage

import (
	"sync"

	"github.com/gsb/coordinator/internal/domain"
)

// Store is a concurrency-safe registry of streams held in memory.
type Store struct {
	mu      sync.RWMutex
	entries map[string]*entry
}

type entry struct {
	mu     sync.Mutex
	stream *domain.Stream
}

// New returns an empty store.
func New() *Store {
	return &Store{entries: make(map[string]*entry)}
}

// GetOrCreate atomically fetches the stream named id, or creates it via build
// if absent. created reports whether a new stream was made. build is only
// invoked while holding the registry lock, so creation of a given id happens
// at most once even under concurrent callers.
func (s *Store) GetOrCreate(id string, build func() (*domain.Stream, error)) (st *domain.Stream, created bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[id]; ok {
		return e.stream, false, nil
	}
	built, err := build()
	if err != nil {
		return nil, false, err
	}
	s.entries[id] = &entry{stream: built}
	return built, true, nil
}

// With runs fn while holding the per-stream lock, guaranteeing that the
// mutation and any snapshot taken inside fn are atomic with respect to other
// operations on the same stream. ok is false when the stream does not exist.
func (s *Store) With(id string, fn func(*domain.Stream)) (ok bool) {
	s.mu.RLock()
	e, exists := s.entries[id]
	s.mu.RUnlock()
	if !exists {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	fn(e.stream)
	return true
}
