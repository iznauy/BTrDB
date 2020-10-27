package magic

import (
	"github.com/iznauy/BTrDB/conf"
	"github.com/iznauy/BTrDB/qtree"
	"sync"
	"time"
)

type BufferType int

var (
	Slice             BufferType = 1
	PreAllocatedSlice BufferType = 2
	LinkedList        BufferType = 3
)

type ApplicationStatistics struct {
	buffer      *bufferStatistics
	cache       *cacheStatistics
	dataTraffic *dataTrafficStatistics
}

type BufferStatisticsEntry struct {
	NewTree     bool 	// 当前试卷序列是否是一个新创建的时间序列
	Count       uint64 	// 当前 buffer 中点的数量
	Size        uint64		// 当前的 buffer 的总的大小
	CreatedAt   time.Time	// 当前 buffer 被分配的时间
}

type bufferStatisticsEntry struct {
	newTree     bool
	count       uint64
	size        uint64
	createdAt   time.Time
}

type bufferStatistics struct {
	entries    map[[16]byte]*bufferStatisticsEntry
	entryLocks map[[16]byte]*sync.Mutex

	mu sync.Mutex
}

type CacheStatistics struct {
	CacheSize    uint64
	CacheHit     uint64
	CacheMiss    uint64
	LeafCount    uint64
	NonLeafCount uint64
}

type cacheStatistics struct {
	cacheSize    uint64
	cacheHit     uint64
	cacheMiss    uint64
	leafCount    uint64
	nonLeafCount uint64
	mu           sync.RWMutex
}

type DataTrafficStatistics struct {
	ReadBytes   int64
	ReadLimiter int64

	WriteBytes   int64
	WriteLimiter int64

	Span time.Duration
}

type dataTrafficStatistics struct {
	totalReadBytes  int64
	totalWriteBytes int64
	totalSpan       time.Duration

	readBytes   int64
	readLimiter int64

	writeBytes   int64
	writeLimiter int64

	span time.Duration

	mu sync.RWMutex
}

func (e *Magic) UpdateDataTraffic(ds *DataTrafficStatistics) {
	dss := e.app.dataTraffic
	dss.mu.Lock()

	dss.totalReadBytes += ds.ReadBytes
	dss.totalWriteBytes += ds.WriteBytes
	dss.totalSpan += ds.Span

	dss.readBytes = ds.ReadBytes
	dss.writeBytes = ds.WriteBytes
	dss.readLimiter = ds.ReadLimiter
	dss.writeLimiter = ds.WriteLimiter

	dss.mu.Unlock()
}

func (e *Magic) UpdateCache(cs *CacheStatistics) {
	cache := e.app.cache
	cache.mu.Lock()
	cache.cacheSize = cs.CacheSize
	cache.cacheHit = cs.CacheHit
	cache.cacheMiss = cs.CacheMiss
	cache.leafCount = cs.LeafCount
	cache.nonLeafCount = cs.NonLeafCount
	cache.mu.Unlock()
}

func (e *Magic) CommitBuffer(uuid [16]byte, buffer qtree.Buffer) {

}

func (e *Magic) AllocateBuffer(uuid [16]byte) (BufferType, uint64) {
	entry, lock := e.app.buffer.getBufferStatisticsEntry(uuid)
	lock.Unlock()
	defer lock.Unlock()
	entry.size = uint64(*conf.Configuration.Coalescence.Interval)
	return Slice, entry.size
}

func (e *Magic) RegisterBuffer(uuid [16]byte) {
	buffers := e.app.buffer
	buffers.mu.Lock()


	buffer := &bufferStatisticsEntry{
		newTree: true,
		count: 0,
		size: 0,
	}
	buffers.entries[uuid] = buffer
	buffers.entryLocks[uuid] = &sync.Mutex{}

	buffers.mu.Unlock()
}


func (b *bufferStatistics) getBufferStatisticsEntry(uuid [16]byte) (*bufferStatisticsEntry, *sync.Mutex) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if entry, ok := b.entries[uuid]; ok {
		return entry, b.entryLocks[uuid]
	}
	return nil, nil
}

func (e *Magic) UpdateBuffer(entry *BufferStatisticsEntry) {

}

func newApplicationStatistics() *ApplicationStatistics {
	return &ApplicationStatistics{
		buffer: newBufferStatistics(),
		cache: newCacheStatistics(),
		dataTraffic: newDataTrafficStatistics(),
	}
}

func newBufferStatistics() *bufferStatistics {
	return &bufferStatistics{}
}

func newCacheStatistics() *cacheStatistics {
	return &cacheStatistics{}
}

func newDataTrafficStatistics() *dataTrafficStatistics {
	return &dataTrafficStatistics{}
}