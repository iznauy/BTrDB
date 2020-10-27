package qtree

import "time"

type TreePolicy interface {
	DecideKAndV(buffer Buffer, span time.Duration) (uint16, uint32)
}

type NaiveTreePolicy struct {}

func (NaiveTreePolicy) DecideKAndV(buffer Buffer, span time.Duration) (uint16, uint32) {
	return 64, 1024
}