package connection

import (
	"encoding/json"
	"sync"
)

// HandshakeStore keeps the latest HELLO response per peer sys-id.
type HandshakeStore struct {
	mu      sync.RWMutex
	entries map[string]json.RawMessage
}

// NewHandshakeStore creates a thread-safe handshake response store.
func NewHandshakeStore() *HandshakeStore {
	return &HandshakeStore{
		entries: make(map[string]json.RawMessage),
	}
}

// Save stores the latest response for a peer sys-id.
func (s *HandshakeStore) Save(sysID string, payload json.RawMessage) {
	if sysID == "" || len(payload) == 0 {
		return
	}

	s.mu.Lock()
	s.entries[sysID] = append(json.RawMessage(nil), payload...)
	s.mu.Unlock()
}

// List returns all stored handshake responses.
func (s *HandshakeStore) List() []json.RawMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()

	responses := make([]json.RawMessage, 0, len(s.entries))
	for _, resp := range s.entries {
		responses = append(responses, append(json.RawMessage(nil), resp...))
	}

	return responses
}
