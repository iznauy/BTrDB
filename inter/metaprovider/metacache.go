package metaprovider

import (
	"github.com/cespare/xxhash"
	"sync"
)

type metaCache struct {
	bucketSize uint64
	buckets    []*bucket
}

type bucket struct {
	mu        sync.RWMutex
	cacheSize uint64
	currSize  uint64
	cache     map[string]map[uint64]*Meta
	p         BatchInsertMetaProvider
}

func (m *metaCache) getBucket(id string) *bucket {
	return m.buckets[xxhash.Sum64String(id)%uint64(m.bucketSize)]
}

func (m *metaCache) get(id string, version uint64) (*Meta, bool) {
	return m.getBucket(id).get(id, version)
}

func (m *metaCache) set(id string, version uint64, met *Meta) error {
	return m.getBucket(id).set(id, version, met)
}

func (b *bucket) get(id string, version uint64) (*Meta, bool) {
	b.mu.RLock()
	if versionMap, ok := b.cache[id]; ok {
		if version > 0 {
			if met, ok := versionMap[version]; ok {
				b.mu.RUnlock()
				return met, true
			}
		} else { // version 为 0 时候表示最新版本
			for v, _ := range versionMap { // 找出最大的版本号
				if v > version {
					version = v
				}
			}
			return versionMap[version], true
		}
	}
	b.mu.RUnlock()
	return nil, false
}

func (b *bucket) set(id string, version uint64, met *Meta) error {
	b.mu.Lock()
	versionMap, ok := b.cache[id]
	if !ok {
		versionMap = make(map[uint64]*Meta, 10)
	}
	versionMap[version] = met
	b.cache[id] = versionMap
	b.currSize += 1
	if b.currSize >= b.cacheSize {
		metaList := make([]*Meta, 0, b.currSize)
		for _, versionMap := range b.cache {
			for _, me := range versionMap {
				metaList = append(metaList, me)
			}
		}
		if err := b.p.BatchInsertMeta(metaList); err != nil {
			b.mu.Unlock()
			return err
		}
		b.currSize = 0
		b.cache = make(map[string]map[uint64]*Meta, b.cacheSize)
	}
	b.mu.Unlock()
	return nil
}

func newBucket(cacheSize uint64, p BatchInsertMetaProvider) *bucket {
	return &bucket{
		cacheSize: cacheSize,
		currSize:  0,
		cache:     make(map[string]map[uint64]*Meta, cacheSize),
		p:         p,
	}
}

func newCache(bucketSize uint64, cacheSize uint64, p BatchInsertMetaProvider) *metaCache {
	buckets := make([]*bucket, 0, bucketSize)
	for i := 0; i < int(bucketSize); i++ {
		buckets = append(buckets, newBucket(cacheSize, p))
	}
	return &metaCache{
		bucketSize: bucketSize,
		buckets:    buckets,
	}
}
