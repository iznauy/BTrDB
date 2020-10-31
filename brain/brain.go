package brain

import (
	"github.com/iznauy/BTrDB/brain/event"
	"github.com/iznauy/BTrDB/brain/handler"
	"github.com/iznauy/BTrDB/brain/stats"
	"github.com/pborman/uuid"
)

var B *Brain

func init() {
	B = &Brain{
		systemStats:      stats.NewSystemStats(),
		osStats:          stats.NewOsStats(),
		applicationStats: stats.NewApplicationStats(),
		handlers:         map[event.EventType][]handler.EventHandler{},
	}
	B.handlers[event.ReadRequest] = []handler.EventHandler{handler.NewReadRequestEventHandler()}
	B.handlers[event.WriteRequest] = []handler.EventHandler{handler.NewWriteRequestEventHandler()}
	B.handlers[event.ReadBlock] = []handler.EventHandler{handler.NewReadBlockEventHandler()}
	B.handlers[event.WriteBlock] = []handler.EventHandler{handler.NewWriteBlockEventHandler()}
	B.handlers[event.CreateBuffer] = []handler.EventHandler{handler.NewCreateBufferEventHandler()}
	B.handlers[event.AppendBuffer] = []handler.EventHandler{handler.NewAppendBufferEventHandler()}
	B.handlers[event.CommitBuffer] = []handler.EventHandler{handler.NewCommitBufferEventHandler()}
	B.handlers[event.CacheNotice] = []handler.EventHandler{handler.NewCacheNoticeEventHandler()}
}

type Brain struct {
	systemStats      *stats.SystemStats
	osStats          *stats.OsStats
	applicationStats *stats.ApplicationStats

	handlers map[event.EventType][]handler.EventHandler
}

func (b *Brain) Emit(event *event.Event) {
	if event == nil {
		return
	}
	for _, h := range b.handlers[event.Type] {
		if !h.Process(event) {
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

func (b *Brain) GetBufferType(id uuid.UUID) BufferType {
	return Slice
}

func (b *Brain) GetCacheMaxSize() uint64 {
	return 10000
}

func (b *Brain) GetKAndFForNewTimeSeries() (K uint16, F uint32) {
	return 6, 1024
}
