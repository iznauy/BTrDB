package handler

import (
	"github.com/iznauy/BTrDB/brain"
	"github.com/iznauy/BTrDB/brain/stats"
	"github.com/iznauy/BTrDB/brain/tool"
	"github.com/iznauy/BTrDB/brain/types"
	"github.com/pborman/uuid"
)

type ReadRequestEventHandler struct {}

func NewReadRequestEventHandler() types.EventHandler {
	return &ReadRequestEventHandler{}
}

func (ReadRequestEventHandler) Process(e *types.Event) bool {

	return true
}

type WriteRequestEventHandler struct {}

func NewWriteRequestEventHandler() types.EventHandler {
	return &WriteRequestEventHandler{}
}

func (WriteRequestEventHandler) Process(e *types.Event) bool {
	span, _ := tool.GetInt64FromMap(e.Params, "span")
	count, _ := tool.GetInt64FromMap(e.Params, "count")

	systemStats := brain.B.SystemStats
	ts := systemStats.GetTs(tool.UUIDToMapKey(e.Source))
	id := uuid.UUID(e.Source).String()
	tsStatsNode := ts.StatsList.Tail
	if tsStatsNode.Prev != nil {
		// 这个事件有可能是在前一个周期上报的
		prevTsStats := tsStatsNode.Prev.Data
		if prevTsStats.EndTime.After(e.Time) {
			if !prevTsStats.Closed {
				// 上一个阶段还没有彻底封口，还需要补偿一个请求才会封口
				prevTsStats.AddRecord(&stats.Record{
					Time: e.Time,
					Size: count,
					ConsumingTime: span,
				}, id)
				return true
			}
		}
	}
	// 这个事件一定是在本周期内插入的（虽然可能是在上个周期内到达的）
	tsStats := tsStatsNode.Data
	tsStats.AddRecord(&stats.Record{
		Time: e.Time,
		Size: count,
		ConsumingTime: span,
	}, id)
	return true
}

