package bstore

type CachePolicy interface {
	InitPolicy(store *BlockStore)
	GetCacheSize() uint64
}

type NaiveCachePolicy struct {
	store *BlockStore
}

func (p *NaiveCachePolicy) InitPolicy(store *BlockStore) {
	p.store = store
}

func (p *NaiveCachePolicy) GetCacheSize() uint64 {
	return p.store.cachemax
}


