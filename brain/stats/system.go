package stats

import (
	"github.com/iznauy/BTrDB/brain/types"
	"sync"
	"time"
)

type SystemStats struct {
	Buffer      *BufferStats
	BufferMutex sync.RWMutex

	Cache      *CacheStats
	CacheMutex sync.RWMutex

	Storage      *StorageStats
	StorageMutex sync.RWMutex
}

type BufferStats struct {
	TotalAllocatedSpace uint64
	TotalUsedSpace      uint64
	TimeSeriesInMemory  uint64

	TsBufferMap map[[16]byte]*TsBufferStats
}

type TsBufferStats struct {
	MaxSize        uint64
	AllocatedSpace uint64
	UsedSpace      uint64
	CommitInterval uint64
	Type           types.BufferType

	LatestCommitted time.Time
}

type CacheStats struct {
	CacheHit     uint64
	CacheMiss    uint64
	CacheSize    uint64
	LeafCount    uint64
	NonLeafCount uint64

	RecentCacheHit  uint64
	RecentCacheMiss uint64

	LatestUpdateTime time.Time
}

type StorageStats struct {
	TotalWriteBlocks uint64
	TotalReadBlocks  uint64
	TotalSpan        time.Duration

	LatestUpdateTime time.Time
}

func NewSystemStats() *SystemStats {
	return &SystemStats{
		Storage: NewStorageStats(),
		Cache:   NewCacheStats(),
		Buffer:  NewBufferStats(),
	}
}

func NewStorageStats() *StorageStats {
	return &StorageStats{
		TotalSpan:        0 * time.Microsecond,
		LatestUpdateTime: time.Now(),
	}
}

func NewCacheStats() *CacheStats {
	return &CacheStats{
		LatestUpdateTime: time.Now(),
	}
}

func NewBufferStats() *BufferStats {
	return &BufferStats{
		TsBufferMap: map[[16]byte]*TsBufferStats{},
	}
}

func NewTsBufferStats() *TsBufferStats {
	return &TsBufferStats{

	}
}