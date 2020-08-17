package metaprovider

import (
	"github.com/cespare/xxhash"
	"sync"
)

type MemMetaProvider struct {
	bucketSize uint64
	buckets    []*memBucket
}

func NewMemMetaProvider(_ map[string]string) (*MemMetaProvider, error) {
	bucketSize := 512
	buckets := make([]*memBucket, 0, bucketSize)
	for i := 0; i < bucketSize; i++ {
		buckets = append(buckets, newMemBucket())
	}
	return &MemMetaProvider{
		bucketSize: uint64(bucketSize),
		buckets:    buckets,
	}, nil
}

func (mem *MemMetaProvider) getBucket(id string) *memBucket {
	return mem.buckets[xxhash.Sum64String(id)%uint64(mem.bucketSize)]
}

func (mem *MemMetaProvider) GetMeta(id string, version uint64) (*Meta, error) {
	m, ok := mem.getBucket(id).get(id, version)
	if !ok {
		return nil, MetaNotFound
	}
	return m, nil
}

func (mem *MemMetaProvider) GetLatestMeta(id string) (*Meta, error) {
	m, ok := mem.getBucket(id).get(id, 0)
	if !ok {
		return nil, MetaNotFound
	}
	return m, nil
}

func (mem *MemMetaProvider) InsertMeta(m *Meta) error {
	mem.getBucket(m.Uuid).add(m)
	return nil
}

type memBucket struct {
	mu      sync.RWMutex
	entries map[string]*entry
}

func newMemBucket() *memBucket {
	return &memBucket{
		entries: make(map[string]*entry, 100),
	}
}

func (mem *memBucket) get(id string, version uint64) (*Meta, bool) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	if ent, ok := mem.entries[id]; ok {
		return ent.get(version)
	}
	return nil, false
}

func (mem *memBucket) add(m *Meta) {
	mem.mu.Lock()
	ent, ok := mem.entries[m.Uuid]
	if !ok {
		ent = newEntry()
		mem.entries[m.Uuid] = ent
	}
	ent.add(m)
	mem.mu.Unlock()
}

type entry struct {
	versions map[uint64]*Meta
	latest   *Meta
}

func newEntry() *entry {
	return &entry{
		versions: make(map[uint64]*Meta, 100),
		latest:   nil,
	}
}

func (e *entry) get(version uint64) (*Meta, bool) {
	if version == 0 {
		if e.latest == nil {
			return nil, false
		}
		return e.latest, true
	}
	m, ok := e.versions[version]
	return m, ok
}

func (e *entry) add(m *Meta) {
	v := m.Version
	e.versions[v] = m
	if e.latest == nil || e.latest.Version < v {
		e.latest = m
	}
}
