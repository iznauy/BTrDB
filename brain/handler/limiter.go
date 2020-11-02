package handler

import "github.com/iznauy/BTrDB/brain/event"

type RateLimiterEventHandler struct {}

func NewRateLimiterEventHandler() EventHandler {
	return &RateLimiterEventHandler{}
}

func (RateLimiterEventHandler) Process(e *event.Event) bool {

	return true
}
