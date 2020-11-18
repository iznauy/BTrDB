package types

type ActionType int

var (
	Nil        ActionType = 0
	BufferSize ActionType = 1
	KAndV      ActionType = 2
)
