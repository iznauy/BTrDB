package handler

import (
	"fmt"
	"github.com/iznauy/BTrDB/brain"
	"github.com/iznauy/BTrDB/brain/tool"
	"github.com/iznauy/BTrDB/brain/types"
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
	id := e.Source
	t := e.Time
	span, _ := tool.GetInt64FromMap(e.Params, "span")
	count, _ := tool.GetInt64FromMap(e.Params, "count")
	fmt.Println(id, t, span, count)

	systemStats := brain.B.SystemStats
	ts := systemStats.GetTs(tool.UUIDToMapKey(e.Source))
	tsStatsNode := ts.StatsList.Tail
	ts.
	return true
}

