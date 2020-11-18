package stats

import (
	"github.com/iznauy/BTrDB/brain/types"
	"math/rand"
	"sync"
	"time"
)

type SystemStats struct {
	TsMap map[[16]byte]*Ts
	TsList []*Ts
	TsStatsMu sync.RWMutex

	Buffer      *BufferStats
	BufferMutex sync.RWMutex

	Cache      *CacheStats
	CacheMutex sync.RWMutex

	Storage      *StorageStats
	StorageMutex sync.RWMutex
}

func (system *SystemStats) GetTs(id [16]byte) *Ts {
	system.TsStatsMu.RLock()
	if ts, ok := system.TsMap[id]; ok {
		return ts
	}
	system.TsStatsMu.RUnlock()
	system.TsStatsMu.Lock()
	if ts, ok := system.TsMap[id]; ok {
		system.TsStatsMu.Unlock()
		return ts
	}
	ts := NewTs(id)
	system.TsMap[id] = ts
	system.TsList = append(system.TsList, ts)
	system.TsStatsMu.Unlock()
	return ts
}

func (system *SystemStats) RandomSampleTs(count int) []*Ts {
	system.TsStatsMu.RLock()
	defer system.TsStatsMu.RUnlock()
	if len(system.TsList) <= count {
		return system.TsList[:]
	}
	begin := rand.Int() % (len(system.TsList) - count)
	return system.TsList[begin:begin+count]
}

type BufferStats struct {
	TotalAnnouncedSpace uint64 // 总共许诺出的内存块
	TotalAllocatedSpace uint64 // 已经分配出去的内存块
	TotalUsedSpace      uint64 // 已经使用的内存块
	TimeSeriesInMemory  uint64

	// 为2020年11月18日13:07:24实验新增的统计信息
	TotalCommitInterval uint64 // 所有时间序列的总提交时间，便于计算平均提交时间

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