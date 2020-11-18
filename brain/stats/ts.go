package stats

import (
	"github.com/iznauy/BTrDB/brain/types"
	"sync"
	"time"
)

type Ts struct {
}

type TsStats struct {
	S       *State
	P       *Performance
	A       *Action
	Mutex   sync.RWMutex
	EndTime *time.Time
}

func NewTsStats() *TsStats {
	return &TsStats{
		S: &State{
			Records: make([]*Record, 0, 1),
		},
	}
}

func (stats *TsStats) AddRecord (record *Record) {
	if stats.EndTime != nil { // 封口之后还插入数据，说明这个事件因为提交延迟导致晚于 commit 事件的提交
		stats.Mutex.Lock()
	}
	stats.S.Records = append(stats.S.Records, record)
	if stats.EndTime != nil {
		// 重新计算一些统计信息
		stats.CalculateStatisticsAndPerformance()
		stats.Mutex.Unlock()
	}
}


type TsStatsNode struct {
	Data *TsStats
	Prev *TsStatsNode
	Next *TsStatsNode
}

type TsStatsList struct {
	Size    int64
	Head    *TsStatsNode
	Tail    *TsStatsNode
	MaxSize int64
}

func NewTsStatsList(maxSize int64) *TsStatsList {
	return &TsStatsList{
		Size:    0,
		MaxSize: maxSize,
		Head:    nil,
		Tail:    nil,
	}
}

func (list *TsStatsList) Append(data *TsStats) {
	if data == nil {
		return
	}
	node := &TsStatsNode{
		Data: data,
		Prev: nil,
		Next: nil,
	}
	if list.Size == 0 {
		list.Head = node
		list.Tail = node
	} else {
		node.Prev = list.Tail
		list.Tail.Next = node
		list.Tail = node
	}
	list.Size += 1
	// 如果 list 中的元素超了就把最前面的节点给清理掉
	if list.MaxSize > 0 && list.MaxSize < list.Size {
		list.Head = list.Head.Next
	}
}

func (list *TsStatsList) Foreach(f func(stats *TsStats)) {
	node := list.Head
	for node != nil {
		f(node.Data)
		node = node.Next
	}
}

type State struct {
	Records       []*Record
	SizeMean      float64
	SizeStd       float64
	DeltaTimeMean float64
	DeltaTimeStd  float64
}

type Record struct {
	Time          time.Time // 进行插入的时间
	Size          int64
	ConsumingTime int64 // 插入耗时
}

type Performance struct {
	P float64 // p = \frac{\sum_{j=1}^{q_i} s^{write}_{ij}}{\sum_{j=1}^{q_i} t^{write}_{ij}}
}

type Action struct {
	Action     types.ActionType
	K          uint16
	V          uint32
	BufferSize uint64
}


func (stats *TsStats) CalculateStatisticsAndPerformance() {
	if len(stats.S.Records) == 0 {
		return
	}
	SizeSum := int64(0)
	LastTime := int64(0)
	for i, record := range stats.S.Records {
		SizeSum += record.Size
		if i > 0 {

		}
		LastTime = record.Time.UnixNano() / 1e3
	}
}
