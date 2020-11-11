package types

type BufferType int

var (
	None              BufferType = 0
	Slice             BufferType = 1
	PreAllocatedSlice BufferType = 2
	LinkedList        BufferType = 3
)
