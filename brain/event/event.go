package event

import "time"

type EventType int

var (
	ReadRequest  EventType = 1
	WriteRequest EventType = 2
	CreateBuffer EventType = 3
	AppendBuffer EventType = 4
	CommitBuffer EventType = 5
	WriteBlock   EventType = 6
	ReadBlock    EventType = 7
	CacheNotice  EventType = 8
)

type Event struct {
	Type   EventType
	Source [16]byte
	Time   time.Time
	Params map[string]interface{}
}
