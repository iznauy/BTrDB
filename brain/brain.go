package brain

import (
	"github.com/iznauy/BTrDB/brain/stats"
	"github.com/iznauy/BTrDB/brain/types"
	"github.com/pborman/uuid"
)

var B *Brain

func init() {
	B = &Brain{
		SystemStats:      stats.NewSystemStats(),
		ApplicationStats: stats.NewApplicationStats(),
		handlers:         map[types.EventType][]types.EventHandler{},
	}
}

type Brain struct {
	SystemStats      *stats.SystemStats
	ApplicationStats *stats.ApplicationStats

	handlers map[types.EventType][]types.EventHandler
}

func (b *Brain) Emit(e *types.Event) {
	if e == nil {
		return
	}
	if e.Type != types.AppendBuffer && e.Type != types.CommitBuffer && e.Type != types.CreateBuffer {
		return
	}
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
	return 10000
}

func (b *Brain) GetBufferMaxSize(id uuid.UUID) uint64 {
	return 10000
}

func (b *Brain) GetBufferType(id uuid.UUID) types.BufferType {
	return types.Slice
}

func (b *Brain) GetCacheMaxSize() uint64 {
	return 10000
}

func (b *Brain) GetKAndFForNewTimeSeries(id uuid.UUID) (K uint16, F uint32) {
	return 6, 1024
}

func (b *Brain) RegisterEventHandler(tp types.EventType, handler types.EventHandler) {

}