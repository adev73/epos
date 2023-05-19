package mem_store

import (
	"fmt"

	"github.com/akrennmair/epos"
	"golang.org/x/exp/maps"
)

type MemoryStorageBackend struct {
	store map[string][]byte
}

func transformFunc(s string) []string {
	// special case for internal data
	if s == "_next_id" {
		return []string{}
	}

	data := ""
	if len(s) < 4 {
		data = fmt.Sprintf("%04s", s)
	} else {
		data = s[len(s)-4:]
	}

	return []string{data[2:4], data[0:2]}
}

func NewMemoryStorageBackend(path string) epos.StorageBackend {
	memStore := &MemoryStorageBackend{
		store: make(map[string][]byte),
	}

	return memStore
}

func (s *MemoryStorageBackend) Read(key string) ([]byte, error) {
	return s.store[key], nil
}

func (s *MemoryStorageBackend) Write(key string, value []byte) error {
	s.store[key] = value
	return nil
}

func (s *MemoryStorageBackend) Erase(key string) error {
	delete(s.store, key)
	return nil
}

func (s *MemoryStorageBackend) Keys() <-chan string {
	return maps.Keys(s.store)
}
