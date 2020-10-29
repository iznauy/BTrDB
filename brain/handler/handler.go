package handler

import "github.com/iznauy/BTrDB/brain/event"

type EventHandler interface {
	Process(event *event.Event) bool
}
