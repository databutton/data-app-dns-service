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

func (l *SafeStringMap) GetMap() map[string]string {
	l.mut.RLock()
	defer l.mut.RUnlock()
	return l.data
}

func (l *SafeStringMap) SetMap(data map[string]string) {
	l.mut.Lock()
	defer l.mut.Unlock()
	l.data = data
}
