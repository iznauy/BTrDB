package types

type EventHandler interface {
	Process(event *Event) bool
}
