package btrdbd

import (
	"github.com/iznauy/BTrDB/inter/bstore"
	"math/rand"
	"time"
)

type BufferType int

var (
	Slice             BufferType = 1
	PreAllocatedSlice BufferType = 2
	LinkedList        BufferType = 3
)

type BufferPolicy interface {
	InitPolicy(*Quasar, *openTree)

	CommitNotice()
	AllocationNotice()

	IsNewTree() bool
	GetEarlyTripTime() uint64
	GetBufferMaxSize() uint64
	GetBufferType() BufferType
}

type NaiveBufferPolicy struct {
	q  *Quasar
	tr *openTree

	newTree   bool // 新的时间序列，强制提交时间相对久一些，并且在建树的时候按照
	earlyTrip uint64
	interval  uint64 // 定期提交实际上不应该被优化
	freq      float64
	decay     float64

	begin time.Time // 本批次数据中第一个数据点到达的时间
}

func (p *NaiveBufferPolicy) InitPolicy(q *Quasar, tr *openTree) {
	p.begin = time.Now()
	sb := p.q.bs.LoadSuperblock(p.tr.id, bstore.LatestGeneration)
	if sb == nil {
		p.newTree = false // TODO: 应当从 superblock 加载相关统计信息
		if p.q.cfg.TransactionCoalesceEnable {
			p.earlyTrip = p.q.cfg.TransactionCoalesceEarlyTrip
			p.interval = p.q.cfg.TransactionCoalesceInterval
		}
		return
	}
	p.newTree = true
	p.earlyTrip = 1024 * 1024 / 16 // 默认 1 MB
	p.interval = 100000            // 默认 100 秒
	p.freq = 0.0
	p.decay = 0.8
	return
}

func (p *NaiveBufferPolicy) CommitNotice() {
	if p.newTree {
		p.newTree = false
		p.freq = float64(p.tr.store.Len()) / time.Now().Sub(p.begin).Seconds()
		if p.q.cfg.TransactionCoalesceEnable {
			p.earlyTrip = p.q.cfg.TransactionCoalesceEarlyTrip
			p.interval = p.q.cfg.TransactionCoalesceInterval
		}
		return
	}
	p.freq = p.decay * p.freq + (1 - p.decay) * (float64(p.tr.store.Len()) / time.Now().Sub(p.begin).Seconds())
	p.earlyTrip = uint64(p.freq * float64(p.interval) + 10)
}

func (p *NaiveBufferPolicy) AllocationNotice() {
	p.begin = time.Now()
}

func (p *NaiveBufferPolicy) IsNewTree() bool {
	return p.newTree
}

func (p *NaiveBufferPolicy) GetEarlyTripTime() uint64 {
	return p.interval
}

func (p *NaiveBufferPolicy) GetBufferMaxSize() uint64 {
	return p.earlyTrip
}

func (p *NaiveBufferPolicy) GetBufferType() BufferType {
	num := rand.Int31()
	if num%3 == 1 {
		return PreAllocatedSlice
	} else if num%3 == 2 {
		return Slice
	}
	return LinkedList
}
