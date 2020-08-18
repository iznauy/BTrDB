package qtree

import (
	"container/list"
	"time"
)

type Buffer interface {
	Get(i int) Record
	Len() int
	Write(r []Record) Buffer
	ToSlice() []Record
}

type SliceBuffer []Record

func NewSliceBuffer() Buffer {
	return SliceBuffer(make([]Record, 20))
}

func NewPreAllocatedSliceBuffer(bufferSize uint64) Buffer {
	return SliceBuffer(make([]Record, bufferSize))
}

func (b SliceBuffer) Get(i int) Record {
	if i < 0 || i >= len(b) {
		panic("index out of bound!")
	}
	return b[i]
}

func (b SliceBuffer) Len() int {
	return len(b)
}

func (b SliceBuffer) Write(records []Record) Buffer {
	return SliceBuffer(append([]Record(b), records...))
}

func (b SliceBuffer) ToSlice() []Record {
	return []Record(b)
}

type LinkedListBuffer struct {
	recordList *list.List
}

func NewLinkedListBuffer() Buffer {
	return &LinkedListBuffer{
		recordList: list.New(),
	}
}

func (b *LinkedListBuffer) Get(i int) Record {
	if i < 0 || i >= b.recordList.Len() {
		panic("index out of bound!")
	}
	ele := b.recordList.Front()
	for j := 0; j < i; j++ {
		ele = ele.Next()
	}
	return ele.Value.(Record)
}

func (b *LinkedListBuffer) Len() int {
	return b.recordList.Len()
}

func (b *LinkedListBuffer) Write(records []Record) Buffer {
	for _, record := range records {
		b.recordList.PushBack(record)
	}
	return b
}

func (b *LinkedListBuffer) ToSlice() []Record {
	records := make([]Record, b.Len())
	ele := b.recordList.Front()
	for i := 0; i < b.recordList.Len(); i++ {
		records[i] = ele.Value.(Record)
		ele = ele.Next()
	}
	return records
}

func (b *LinkedListBuffer) CreatedAt() time.Duration {

}

func (b *LinkedListBuffer) CommittedAt() time.Duration {

}