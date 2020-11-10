package brain

import (
	"github.com/iznauy/BTrDB/brain/stats"
	"github.com/iznauy/BTrDB/brain/types"
	"github.com/op/go-logging"
	"github.com/pborman/uuid"
	"math/rand"
	"sync"
	"time"
)

var B *Brain

var log *logging.Logger

var (
	appendTimes int64
	appendSpan  time.Duration
	appendMu    sync.RWMutex

	commitTimes int64
	commitSpan  time.Duration
	commitMu    sync.RWMutex

	createTimes int64
	createSpan  time.Duration
	createMu    sync.RWMutex
)

func init() {
	B = &Brain{
		SystemStats:      stats.NewSystemStats(),
		ApplicationStats: stats.NewApplicationStats(),
		handlers:         map[types.EventType][]types.EventHandler{},
	}
	log = logging.MustGetLogger("log")
	go BroadcastStatistics()
}

type Brain struct {
	SystemStats      *stats.SystemStats
	ApplicationStats *stats.ApplicationStats

	handlers map[types.EventType][]types.EventHandler
}

func BroadcastStatistics() {
	for {
		time.Sleep(5 * time.Second) // 每隔五秒输出一次统计信息

		appendMu.RLock()
		if appendTimes > 0 {
			log.Infof("append 事件共触发了%v次，总耗时%vms，平均耗时%v微秒.", appendTimes, appendSpan.Milliseconds(), appendSpan.Microseconds() * 1.0 / appendTimes)
		} else {
			log.Info("append 事件尚未触发")
		}
		appendMu.RUnlock()

		createMu.RLock()
		if createTimes > 0 {
			log.Infof("create 事件共触发了%v次，总耗时%vms，平均耗时%v微秒", createTimes, createSpan.Milliseconds(), createSpan.Microseconds() * 1.0 / createTimes)
		} else {
			log.Info("create 事件尚未触发")
		}
		createMu.RUnlock()

		commitMu.RLock()
		if commitTimes > 0 {
			log.Infof("commit 事件共触发了%v次，总耗时%vms，平均耗时%v微秒", commitTimes, commitSpan.Milliseconds(), commitSpan.Microseconds() * 1.0 / commitTimes)
		} else {
			log.Info("commit 事件尚未触发")
		}
		commitMu.RUnlock()

		B.SystemStats.BufferMutex.RLock()
		buffer := B.SystemStats.Buffer
		log.Infof("buffer 统计信息：总使用的空间 = %v，总分配的空间 = %v, 内存中驻留的时间序列 = %v", buffer.TotalUsedSpace, buffer.TotalAllocatedSpace, buffer.TimeSeriesInMemory)
		B.SystemStats.BufferMutex.RUnlock()
	}
}

func (b *Brain) Emit(e *types.Event) {
	if e == nil {
		return
	}
	if e.Type != types.AppendBuffer && e.Type != types.CommitBuffer && e.Type != types.CreateBuffer {
		return
	}
	defer func() func() {
		begin := time.Now()
		return func() {
			localSpan := time.Now().Sub(begin)
			if e.Type == types.AppendBuffer {
				appendMu.Lock()
				appendSpan += localSpan
				appendTimes += 1
				appendMu.Unlock()
			}
			if e.Type == types.CommitBuffer {
				commitMu.Lock()
				commitSpan += localSpan
				commitTimes += 1
				commitMu.Unlock()
			}
			if e.Type == types.CreateBuffer {
				createMu.Lock()
				createSpan += localSpan
				createTimes += 1
				createMu.Unlock()
			}
		}
	} ()()
	for _, h := range b.handlers[e.Type] {
		if !h.Process(e) {
			break
		}
	}
}

func (b *Brain) GetReadAndWriteLimiter() (int64, int64) {
	return 1000000000, 1000000000
}

func (b *Brain) GetCommitInterval(id uuid.UUID) uint64 {
	return 10000 + uint64(rand.Int() % 10000)
}

func (b *Brain) GetBufferMaxSize(id uuid.UUID) uint64 {
	return 10000 + uint64(rand.Int() % 10000)
}

func (b *Brain) GetBufferType(id uuid.UUID) types.BufferType {
	return types.Slice
}

func (b *Brain) GetCacheMaxSize() uint64 {
	return 125000
}

func (b *Brain) GetKAndFForNewTimeSeries(id uuid.UUID) (K uint16, F uint32) {
	return 64, 1024
}

func (b *Brain) RegisterEventHandler(tp types.EventType, handler types.EventHandler) {
	handlerMap := b.handlers
	handlers, ok := handlerMap[tp]
	if !ok {
		handlers = make([]types.EventHandler, 0, 1)
	}
	handlers = append(handlers, handler)
	handlerMap[tp] = handlers
}
