package handler

import (
	"github.com/iznauy/BTrDB/brain"
	"github.com/iznauy/BTrDB/brain/tool"
	"github.com/iznauy/BTrDB/brain/types"
)

type ReadBlockEventHandler struct {}

func NewReadBlockEventHandler() types.EventHandler {
	return &ReadBlockEventHandler{}
}

func (ReadBlockEventHandler) Process(e *types.Event) bool {
	now := e.Time
	count := uint64(0)
	if n, ok := tool.GetUint64FromMap(e.Params, "core_count"); ok {
		count += n
	}
	if n, ok := tool.GetUint64FromMap(e.Params, "vector_count"); ok {
		count += n
	}
	systemStats := brain.B.SystemStats
	systemStats.StorageMutex.Lock()
	defer systemStats.StorageMutex.Unlock()

	storage := systemStats.Storage
	storage.TotalSpan += now.Sub(storage.LatestUpdateTime)
	storage.TotalReadBlocks += count
	storage.LatestUpdateTime = now
	return true
}

type WriteBlockEventHandler struct {}

func NewWriteBlockEventHandler() types.EventHandler {
	return &WriteBlockEventHandler{}
}

func (WriteBlockEventHandler) Process(e *types.Event) bool {
	now := e.Time
	count := uint64(0)
	if n, ok := tool.GetUint64FromMap(e.Params, "core_count"); ok {
		count += n
	}
	if n, ok := tool.GetUint64FromMap(e.Params, "vector_count"); ok {
		count += n
	}
	systemStats := brain.B.SystemStats
	systemStats.StorageMutex.Lock()
	defer systemStats.StorageMutex.Unlock()

	storage := systemStats.Storage
	storage.TotalSpan += now.Sub(storage.LatestUpdateTime)
	storage.TotalWriteBlocks += count
	storage.LatestUpdateTime = now
	return true
}


