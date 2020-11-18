package qtree

import (
	"container/list"
	"github.com/pborman/uuid"
)

type Buffer interface {
	Get(i int) Record
	Len() int
	Write(r []Record) Buffer
	ToSlice() []Record
}

type SliceBuffer struct {
	id      uuid.UUID
	records []Record
}

func NewSliceBuffer(id uuid.UUID) Buffer {
	return NewPreAllocatedSliceBuffer(id, 0)
}

func NewPreAllocatedSliceBuffer(id uuid.UUID, bufferSize uint64) Buffer {
	return &SliceBuffer{
		id:      id,
		records: make([]Record, 0, bufferSize),
	}
}

func (b *SliceBuffer) Get(i int) Record {
	if i < 0 || i >= len(b.records) {
		panic("index out of bound!")
	}
	return b.records[i]
}

func (b *SliceBuffer) Len() int {
	return len(b.records)
}

func (b *SliceBuffer) Write(records []Record) Buffer {
	b.records = append(b.records, records...)
	//e := &types.Event{
	//	Type:   types.AppendBuffer,
	//	Source: b.id,
	//	Time:   time.Now(),
	//	Params: map[string]interface{}{
	//		"space":  uint64(cap(b.records)), // 实际分配的空间数量
	//		"size":   uint64(len(b.records)), // 目前已经有的元素数
	//		"append": uint64(len(records)),   // 本次新增多少数据点
	//	},
	//}
	//brain.B.Emit(e)
	return b
}

func (b *SliceBuffer) ToSlice() []Record {
	return b.records
}

type LinkedListBuffer struct {
	id         uuid.UUID
	recordList *list.List
}

func NewLinkedListBuffer(id uuid.UUID) Buffer {
	return &LinkedListBuffer{
		id:         id,
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
	//e := &types.Event{
	//	Type:   types.AppendBuffer,
	//	Source: b.id,
	//	Time:   time.Now(),
	//	Params: map[string]interface{}{
	//		"space":  b.recordList.Len(), // 实际分配的空间数量
	//		"size":   b.recordList.Len(), // 目前已经有的元素数
	//		"append": len(records),       // 本次新增多少数据点
	//	},
	//}
	//brain.B.Emit(e)
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
