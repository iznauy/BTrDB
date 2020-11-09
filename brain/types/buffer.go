package types

type BufferType int

var (
	Slice             BufferType = 1
	PreAllocatedSlice BufferType = 2
	LinkedList        BufferType = 3
)