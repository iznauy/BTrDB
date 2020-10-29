package handler

import "github.com/iznauy/BTrDB/brain/event"

type CreateBufferEventHandler struct {}

func NewCreateBufferEventHandler() EventHandler {
	return &CreateBufferEventHandler{}
}

func (CreateBufferEventHandler) Process(e *event.Event) bool {
	return true
}

type AppendBufferEventHandler struct {}

func NewAppendBufferEventHandler() EventHandler {
	return &AppendBufferEventHandler{};
}

func (AppendBufferEventHandler) Process(e *event.Event) bool {
	return true
}

type CommitBufferEventHandler struct{}

func NewCommitBufferEventHandler() EventHandler {
	return &CommitBufferEventHandler{}
}

func (CommitBufferEventHandler) Process(e *event.Event) bool {
	return true
}
