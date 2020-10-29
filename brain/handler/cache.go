package handler

import "github.com/iznauy/BTrDB/brain/event"

type CacheNoticeEventHandler struct {}

func NewCacheNoticeEventHandler() EventHandler {
	return &CacheNoticeEventHandler{}
}

func (CacheNoticeEventHandler) Process(e *event.Event) bool {
	return true
}
