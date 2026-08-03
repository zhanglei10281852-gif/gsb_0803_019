package store

import (
	"context"
	"sync"

	"livecoord/internal/domain"
)

type StreamStore interface {
	CreateStream(ctx context.Context, s *domain.Stream) error
	GetStream(ctx context.Context, streamID string) (*domain.Stream, bool)
}

type MemoryStore struct {
	mu      sync.RWMutex
	streams map[string]*domain.Stream
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{streams: make(map[string]*domain.Stream)}
}

func (m *MemoryStore) CreateStream(ctx context.Context, s *domain.Stream) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.streams[s.ID]; exists {
		return domain.NewError(domain.ErrCodeStreamExists, "stream already exists: "+s.ID)
	}
	m.streams[s.ID] = s
	return nil
}

func (m *MemoryStore) GetStream(ctx context.Context, streamID string) (*domain.Stream, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.streams[streamID]
	return s, ok
}
