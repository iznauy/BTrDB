package handler

import "github.com/iznauy/BTrDB/brain/event"

type ReadBlockEventHandler struct {}

func NewReadBlockEventHandler() EventHandler {
	return &ReadBlockEventHandler{}
}

func (ReadBlockEventHandler) Process(e *event.Event) bool {
	return true
}

type WriteBlockEventHandler struct {}

func NewWriteBlockEventHandler() EventHandler {
	return &WriteBlockEventHandler{}
}

func (WriteBlockEventHandler) Process(e *event.Event) bool {
	return true
}


