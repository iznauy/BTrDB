package qtree

import "time"

type QTreePolicy interface {
	DecideKAndV(buffer Buffer, span time.Duration) (uint16, uint32)
}

type NaiveQTreePolicy struct {}

func (NaiveQTreePolicy) DecideKAndV(buffer Buffer, span time.Duration) (uint16, uint32) {
	return 64, 1024
}