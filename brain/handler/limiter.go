package handler

import (
	"github.com/iznauy/BTrDB/brain/types"
)

type RateLimiterEventHandler struct {}

func NewRateLimiterEventHandler() types.EventHandler {
	return &RateLimiterEventHandler{}
}

func (RateLimiterEventHandler) Process(e *types.Event) bool {

	return true
}
