package safemap

import "sync"

type SafeStringMap struct {
	data map[string]string
	mut  sync.RWMutex
}

func NewSafeStringMap() *SafeStringMap {
	return &SafeStringMap{
		data: make(map[string]string),
	}
}

// GetMap returns the current map. Threadsafe.
func (l *SafeStringMap) GetMap() map[string]string {
	l.mut.RLock()
	defer l.mut.RUnlock()
	return l.data
}

// SetMap swaps in a new map. Threadsafe.
func (l *SafeStringMap) SetMap(data map[string]string) {
	l.mut.Lock()
	defer l.mut.Unlock()
	l.data = data
}
