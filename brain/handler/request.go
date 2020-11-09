package handler

import (
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
	return true
}

