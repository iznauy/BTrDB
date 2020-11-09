package types

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
	LimitNotice  EventType = 9
)

type Event struct {
	Type   EventType
	Source []byte
	Time   time.Time
	Params map[string]interface{}
}
