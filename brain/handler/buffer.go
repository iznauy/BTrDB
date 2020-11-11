package handler

import (
	"github.com/iznauy/BTrDB/brain"
	"github.com/iznauy/BTrDB/brain/stats"
	"github.com/iznauy/BTrDB/brain/tool"
	"github.com/iznauy/BTrDB/brain/types"
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
	defer systemStats.BufferMutex.Unlock()

	buffer := systemStats.Buffer
	buffer.TimeSeriesInMemory += 1
	buffer.TotalAnnouncedSpace += bufferMaxSize
	if bufferType == types.PreAllocatedSlice { // 只有是预分配切片才会在刚开始的时候实际分配内存出去
		buffer.TotalAllocatedSpace += bufferMaxSize
	}

	tsBufferStats, ok := buffer.TsBufferMap[tool.UUIDToMapKey(e.Source)]
	if !ok {
		tsBufferStats = stats.NewTsBufferStats()
		buffer.TsBufferMap[tool.UUIDToMapKey(e.Source)] = tsBufferStats
	}
	tsBufferStats.Type = bufferType
	tsBufferStats.AllocatedSpace = 0
	if bufferType == types.PreAllocatedSlice {
		tsBufferStats.AllocatedSpace = bufferMaxSize
	}
	tsBufferStats.CommitInterval = bufferCommitInterval
	tsBufferStats.UsedSpace = 0
	tsBufferStats.MaxSize = bufferMaxSize
	return true
}

type AppendBufferEventHandler struct{}

func NewAppendBufferEventHandler() types.EventHandler {
	return &AppendBufferEventHandler{};
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
	defer systemStats.BufferMutex.Unlock()

	buffer := systemStats.Buffer
	tsBufferStats := buffer.TsBufferMap[tool.UUIDToMapKey(e.Source)]
	buffer.TimeSeriesInMemory -= 1
	buffer.TotalAllocatedSpace -= tsBufferStats.AllocatedSpace
	buffer.TotalUsedSpace -= tsBufferStats.UsedSpace
	buffer.TotalAnnouncedSpace -= tsBufferStats.MaxSize

	tsBufferStats.UsedSpace = 0
	tsBufferStats.AllocatedSpace = 0
	tsBufferStats.CommitInterval = 0
	tsBufferStats.MaxSize = 0
	tsBufferStats.LatestCommitted = e.Time
	tsBufferStats.Type = types.None
	return true
}
