package epos

import (
	"fmt"

	"github.com/akrennmair/epos/internal/store_engines/diskv_store"
	"github.com/akrennmair/epos/internal/store_engines/mem_store"
)

type StorageType string

const (
	STORAGE_AUTO  StorageType = "auto"
	STORAGE_DISKV StorageType = "diskv"
	//STORAGE_LEVELDB   StorageType = "leveldb" // Deprecated
	STORAGE_MEMORY StorageType = "memory"
)

type StorageBackend interface {
	Read(key string) ([]byte, error)
	Write(key string, value []byte) error
	Erase(key string) error
	Keys() <-chan string
}

var storageBackends map[StorageType]func(string) StorageBackend

func init() {
	storageBackends = make(map[StorageType]func(string) StorageBackend)
	RegisterStorageBackend(string(STORAGE_DISKV), diskv_store.NewDiskvStorageBackend)
	RegisterStorageBackend(string(STORAGE_MEMORY), mem_store.NewMemoryStorageBackend)
}

// RegisterStorageBackend registers a new custom storage backend under a new
// name. If the name is already used, an error is returned.
//
// In order to create a new custom storage backend, the programmer must also
// provide a function that takes the path where the storage backend must write
// its data (as a single file or within a directory) and that returns an object
// that satisfies the interface StorageBackend
func RegisterStorageBackend(name string, factoryFunc func(path string) StorageBackend) error {
	if _, contains := storageBackends[StorageType(name)]; contains {
		return fmt.Errorf("storage backend %s already registered", name)
	}
	storageBackends[StorageType(name)] = factoryFunc
	return nil
}
