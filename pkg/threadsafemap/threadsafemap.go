package threadsafemap

import (
	"maps"
	"sync"
)

// writeOp represents a write operation to be performed on the map
type writeOp[K comparable, V any] struct {
	key   K
	value V
	done  chan struct{} // optional channel to signal completion
	del   bool
}

// Map is a generic thread-safe map implementation
type Map[K comparable, V any] struct {
	data      map[K]V
	mu        sync.RWMutex
	writeCh   chan writeOp[K, V]
	closeCh   chan struct{}
	closeOnce sync.Once
}

// NewThreadSafeMapFromMap creates a new thread-safe map from an existing map (using a shallow copy)
func NewThreadSafeMapFromMap[K comparable, V any](existing map[K]V) *Map[K, V] {
	var data map[K]V
	if existing == nil {
		data = make(map[K]V)
	} else {
		data = maps.Clone(existing)
	}

	tsm := &Map[K, V]{
		data:    data,
		writeCh: make(chan writeOp[K, V], 100), // buffered channel
		closeCh: make(chan struct{}),
	}

	// Start the write worker goroutine
	go tsm.writeWorker()

	return tsm
}

// New creates a new thread-safe map
func New[K comparable, V any]() *Map[K, V] {
	return NewThreadSafeMapFromMap[K, V](nil)
}

// Get retrieves a value from the map with minimal read lock duration
func (tsm *Map[K, V]) Get(key K) (V, bool) {
	tsm.mu.RLock()
	value, ok := tsm.data[key]
	tsm.mu.RUnlock()
	return value, ok
}

// Set adds or updates a key-value pair in the map asynchronously
func (tsm *Map[K, V]) SetAsync(key K, value V) {
	select {
	case tsm.writeCh <- writeOp[K, V]{key: key, value: value}:
		// Write operation queued successfully
	case <-tsm.closeCh:
		// Map is closed, ignore the write
	}
}

// SetSync adds or updates a key-value pair in the map synchronously
// This method blocks until the write operation is completed
func (tsm *Map[K, V]) SetSync(key K, value V) {
	done := make(chan struct{})
	select {
	case tsm.writeCh <- writeOp[K, V]{key: key, value: value, done: done}:
		<-done // Wait for completion
	case <-tsm.closeCh:
		// Map is closed, ignore the write
	}
}

// Delete removes a key from the map asynchronously
func (tsm *Map[K, V]) DeleteAsync(key K) {
	var zero V
	select {
	case tsm.writeCh <- writeOp[K, V]{key: key, value: zero, del: true}:
		// Delete operation queued successfully
	case <-tsm.closeCh:
		// Map is closed, ignore the operation
	}
}

// DeleteSync removes a key from the map synchronously
func (tsm *Map[K, V]) DeleteSync(key K) {
	done := make(chan struct{})
	var zero V
	select {
	case tsm.writeCh <- writeOp[K, V]{key: key, value: zero, done: done, del: true}:
		<-done // Wait for completion
	case <-tsm.closeCh:
		// Map is closed, ignore the operation
	}
}

// Len returns the current number of elements in the map
func (tsm *Map[K, V]) Len() int {
	tsm.mu.RLock()
	length := len(tsm.data)
	tsm.mu.RUnlock()
	return length
}

// Keys returns a slice of all keys in the map
func (tsm *Map[K, V]) Keys() []K {
	tsm.mu.RLock()
	keys := make([]K, 0, len(tsm.data))
	for k := range tsm.data {
		keys = append(keys, k)
	}
	tsm.mu.RUnlock()
	return keys
}

type KV[K comparable, V any] struct {
	K K
	V V
}

// Items returns a slice of all key,value pairs in the map
func (tsm *Map[K, V]) Items() []KV[K, V] {
	tsm.mu.RLock()
	items := make([]KV[K, V], 0, len(tsm.data))
	for k, v := range tsm.data {
		items = append(items, KV[K, V]{K: k, V: v})
	}
	tsm.mu.RUnlock()
	return items
}

// Close shuts down the map and stops the write worker
func (tsm *Map[K, V]) Close() {
	tsm.closeOnce.Do(func() {
		close(tsm.closeCh)
	})
}

// writeWorker is the goroutine that handles all write operations
func (tsm *Map[K, V]) writeWorker() {
	for {
		select {
		case op := <-tsm.writeCh:
			// Hold write lock for minimal duration
			tsm.mu.Lock()
			if op.del {
				// This is a delete operation
				delete(tsm.data, op.key)
			} else {
				// This is a set operation
				tsm.data[op.key] = op.value
			}
			tsm.mu.Unlock()

			// Signal completion if requested
			if op.done != nil {
				close(op.done)
			}

		case <-tsm.closeCh:
			// Drain remaining operations before closing
			for {
				select {
				case op := <-tsm.writeCh:
					if op.del {
						tsm.mu.Lock()
						delete(tsm.data, op.key)
						tsm.mu.Unlock()
					} else {
						tsm.mu.Lock()
						tsm.data[op.key] = op.value
						tsm.mu.Unlock()
					}

					if op.done != nil {
						close(op.done)
					}
				default:
					return // No more operations to process
				}
			}
		}
	}
}
