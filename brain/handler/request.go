package handler

import "github.com/iznauy/BTrDB/brain/event"

type ReadRequestEventHandler struct {}

func NewReadRequestEventHandler() EventHandler {
	return &ReadRequestEventHandler{}
}

func (ReadRequestEventHandler) Process(e *event.Event) bool {
	return true
}

type WriteRequestEventHandler struct {}

func NewWriteRequestEventHandler() EventHandler {
	return &WriteRequestEventHandler{}
}

func (WriteRequestEventHandler) Process(e *event.Event) bool {
	return true
}

