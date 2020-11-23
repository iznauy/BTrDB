package handler

import (
	"github.com/iznauy/BTrDB/brain"
	"github.com/iznauy/BTrDB/brain/stats"
	"github.com/iznauy/BTrDB/brain/tool"
	"github.com/iznauy/BTrDB/brain/types"
	"time"
)

type CreateBufferEventHandler struct{}

func NewCreateBufferEventHandler() types.EventHandler {
	return &CreateBufferEventHandler{}
}

func (CreateBufferEventHandler) Process(e *types.Event) bool {
	bufferType := e.Params["type"].(types.BufferType)
	bufferMaxSize, _ := tool.GetUint64FromMap(e.Params, "max_size")
	bufferCommitInterval, _ := tool.GetUint64FromMap(e.Params, "commit_interval")

	systemStats := brain.B.SystemStats

	systemStats.BufferMutex.Lock()
	buffer := systemStats.Buffer
	buffer.TimeSeriesInMemory += 1
	buffer.TotalAnnouncedSpace += bufferMaxSize
	buffer.TotalCommitInterval += bufferCommitInterval

	tsBufferStats, ok := buffer.TsBufferMap[tool.UUIDToMapKey(e.Source)]
	if !ok {
		tsBufferStats = stats.NewTsBufferStats()
		buffer.TsBufferMap[tool.UUIDToMapKey(e.Source)] = tsBufferStats
	}
	systemStats.BufferMutex.Unlock()

	tsBufferStats.Type = bufferType
	tsBufferStats.AllocatedSpace = 0
	tsBufferStats.CommitInterval = bufferCommitInterval
	tsBufferStats.UsedSpace = 0
	tsBufferStats.MaxSize = bufferMaxSize

	ts := systemStats.GetTs(tool.UUIDToMapKey(e.Source))
	if ts.StatsList.Size == 0 {
		ts.StatsList.Append(stats.NewTsStats())
	}

	return true
}

type AppendBufferEventHandler struct{}

func NewAppendBufferEventHandler() types.EventHandler {
	return &AppendBufferEventHandler{}
}

func (AppendBufferEventHandler) Process(e *types.Event) bool {
	allocatedSpace, _ := tool.GetUint64FromMap(e.Params, "space")
	usedSpace, _ := tool.GetUint64FromMap(e.Params, "size")
	// appendCount, _ := tool.GetUint64FromMap(e.Params, "append")

	systemStats := brain.B.SystemStats
	systemStats.BufferMutex.Lock()
	defer systemStats.BufferMutex.Unlock()

	buffer := systemStats.Buffer
	tsBufferStats := buffer.TsBufferMap[tool.UUIDToMapKey(e.Source)]
	// 使用差值更新总体的统计信息
	buffer.TotalAllocatedSpace += allocatedSpace - tsBufferStats.AllocatedSpace
	buffer.TotalUsedSpace += usedSpace - tsBufferStats.UsedSpace

	// 更新当前时间序列的数据统计信息
	tsBufferStats.AllocatedSpace = allocatedSpace
	tsBufferStats.UsedSpace = usedSpace
	return true
}

type CommitBufferEventHandler struct{}

func NewCommitBufferEventHandler() types.EventHandler {
	return &CommitBufferEventHandler{}
}

func (CommitBufferEventHandler) Process(e *types.Event) bool {
	systemStats := brain.B.SystemStats
	systemStats.BufferMutex.Lock()

	buffer := systemStats.Buffer
	tsBufferStats := buffer.TsBufferMap[tool.UUIDToMapKey(e.Source)]
	buffer.TimeSeriesInMemory -= 1
	buffer.TotalAnnouncedSpace -= tsBufferStats.MaxSize
	systemStats.BufferMutex.Unlock()

	tsBufferStats.UsedSpace = 0
	tsBufferStats.AllocatedSpace = 0
	tsBufferStats.CommitInterval = 0
	tsBufferStats.MaxSize = 0
	tsBufferStats.LatestCommitted = e.Time
	tsBufferStats.Type = types.None

	ts := systemStats.GetTs(tool.UUIDToMapKey(e.Source))
	tsStats := ts.StatsList.Tail.Data
	endTime := time.Now()
	tsStats.EndTime = &endTime
	full := e.Params["full"].(bool)
	if !full {
		tsStats.Closed = true // 假如是因为超时被强制提交，则直接封口，后续请求无法写入该 tsStats
	}
	return true
}
