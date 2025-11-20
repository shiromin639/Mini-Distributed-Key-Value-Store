package storage

import "sync"

type ValueEntry struct {
	value 	[]byte
	ts 		int64
	origin 	string
}
type Store struct {
	m map[string]ValueEntry
	mu sync.RWMutex
}

func NewStore() *Store {
	return &Store{m : make(map[string]ValueEntry)}
}

func (s *Store) Get(k string) (ValueEntry, bool) {
	s.mu.RLock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok
}

func (s *Store) Put(k string, v []byte, ts int64, origin string) {
	s.mu.Lock();
	defer s.mu.Unlock()
	s.m[k] = ValueEntry{value: v, ts: ts, origin: origin}
}

func (s *Store) Delete(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, k)
}