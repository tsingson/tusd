package memorylockerswiss

import (
	"context"
	"hash/fnv"

	csmap "github.com/mhmtszr/concurrent-swiss-map"
	"github.com/tus/tusd/v2/pkg/handler"
)

// MemoryLocker persists locks using memory and therefore allowing a simple and
// cheap mechanism. Locks will only exist as long as this object is kept in
// reference and will be erased if the program exits.
type MemoryLocker struct {
	// locks map[string]lockEntry
	locks *csmap.CsMap[string, lockEntry]
	// mutex sync.RWMutex
}

type lockEntry struct {
	lockReleased   chan struct{}
	requestRelease func()
}

func initialMap() *csmap.CsMap[string, lockEntry] {
	myMap := csmap.Create[string, lockEntry](
		// set the number of map shards. the default value is 32.
		csmap.WithShardCount[string, lockEntry](32),

		// if don't set custom hasher, use the built-in maphash.
		csmap.WithCustomHasher[string, lockEntry](func(key string) uint64 {
			hash := fnv.New64a()
			hash.Write([]byte(key))
			return hash.Sum64()
		}),

		// set the total capacity, every shard map has total capacity/shard count capacity. the default value is 0.
		csmap.WithSize[string, lockEntry](1000),
	)
	return myMap
}

// New creates a new in-memory locker.
func New() *MemoryLocker {
	return &MemoryLocker{
		// locks: make(map[string]lockEntry),
		locks: initialMap(),
	}
}

// UseIn adds this locker to the passed composer.
func (locker *MemoryLocker) UseIn(composer *handler.StoreComposer) {
	composer.UseLocker(locker)
}

func (locker *MemoryLocker) NewLock(id string) (handler.Lock, error) {
	return memoryLock{locker, id}, nil
}

type memoryLock struct {
	locker *MemoryLocker
	id     string
}

// Lock tries to obtain the exclusive lock.
func (lock memoryLock) Lock(ctx context.Context, requestRelease func()) error {
	// lock.locker.mutex.RLock()
	entry, ok := lock.locker.locks.Load(lock.id)
	// lock.locker.mutex.RUnlock()

requestRelease:
	if ok {
		entry.requestRelease()
		select {
		case <-ctx.Done():
			return handler.ErrLockTimeout
		case <-entry.lockReleased:
		}
	}

	// lock.locker.mutex.Lock()
	// Check that the lock has not already been created in the meantime
	entry, ok = lock.locker.locks.Load(lock.id)
	if ok {
		// Lock has been created in the meantime, so we must wait again until it is free
		// lock.locker.mutex.Unlock()
		goto requestRelease
	}

	// No lock exists, so we can create it
	entry = lockEntry{
		lockReleased:   make(chan struct{}),
		requestRelease: requestRelease,
	}

	lock.locker.locks.Store(lock.id, entry)

	return nil
}

// Unlock releases a lock. If no such lock exists, no error will be returned.
func (lock memoryLock) Unlock() error {
	// lock.locker.mutex.Lock()

	// lockReleased := lock.locker.locks[lock.id].lockReleased
	entry, ok := lock.locker.locks.Load(lock.id)
	if !ok {
		return nil
	}
	lockReleased := entry.lockReleased
	// Delete the lock entry entirely

	lock.locker.locks.Delete(lock.id)
	// lock.locker.mutex.Unlock()

	close(lockReleased)

	return nil
}
