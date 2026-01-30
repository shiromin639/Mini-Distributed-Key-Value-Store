package kv

import (
	"log"
	"sync"
)

type KVStore struct {
	mu   sync.Mutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (kv *KVStore) Get(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.data[key]
}

func (kv *KVStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
	log.Printf("--- STATE MACHINE APPLIED: %s = %s ---", key, value)
}
