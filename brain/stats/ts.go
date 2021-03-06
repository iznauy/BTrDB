package stats

import (
	"fmt"
	"github.com/iznauy/BTrDB/brain/conf"
	"github.com/iznauy/BTrDB/brain/log"
	"github.com/iznauy/BTrDB/brain/types"
	"math"
	"time"
)

var fileLog *log.Logger

func init() {
	fileLog = log.GetLogger()
}

type Ts struct {
	StatsList      *TsStatsList
	ID             [16]byte
	K              uint16
	V              uint32
	CommitInterval uint64
	BufferSize     uint64
	LastCommitTime *time.Time
}

func NewTs(id [16]byte) *Ts {
	return &Ts{
		StatsList:      NewTsStatsList(conf.StateSequenceMaxLength),
		ID:             id,
		LastCommitTime: nil,
	}
}

type TsStats struct {
	S       *State
	P       *Performance
	A       *Action
	Period  int
	EndTime *time.Time
	Closed  bool
}

func NewTsStats() *TsStats {
	return &TsStats{
		S: &State{
			Records: make([]*Record, 0, 1),
		},
		Period: conf.DecisionIntervalPeriods,
		Closed: false,
	}
}

func (stats *TsStats) AddRecord(record *Record, id string) {
	stats.S.Records = append(stats.S.Records, record)
	if stats.EndTime != nil {
		// 封口之后还插入数据，说明这个事件因为提交延迟导致晚于 commit 事件的提交
		// 重新计算一些统计信息
		stats.Closed = true
		if stats.P != nil {
			stats.CalculateStatisticsAndPerformance(id)
		}
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
	// 如果 list 存在元素数量限制，而且当前元素超了就把最前面的节点给清理掉
	if list.MaxSize > 0 && list.MaxSize < list.Size {
		list.Head = list.Head.Next
		list.Size -= 1
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

func (s *State) Distance(anotherS *State) float64 {
	SizeMeanDelta := anotherS.SizeMean - s.SizeMean
	SizeStdDelta := anotherS.SizeStd - s.SizeStd
	DeltaTimeMeanDelta := anotherS.DeltaTimeMean - s.DeltaTimeMean
	DeltaTimeStdDelta := anotherS.DeltaTimeStd - s.DeltaTimeStd
	return SizeMeanDelta*SizeMeanDelta + SizeStdDelta*SizeStdDelta +
		DeltaTimeMeanDelta*DeltaTimeMeanDelta + DeltaTimeStdDelta*DeltaTimeStdDelta
}

func (s *State) String() string {
	return fmt.Sprintf("State: {SizeMean: %f, SizeStd: %f, DeltaTimeMean: %f, DeltaTimeStd: %f}", s.SizeMean, s.SizeStd,
		s.DeltaTimeMean, s.DeltaTimeStd)
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
	Action         types.ActionType
	BufferSize     uint64
	CommitInterval uint64
}

func (stats *TsStats) CalculateStatisticsAndGetP() float64 {
	if len(stats.S.Records) == 0 {
		return 0.0
	}
	SizeSum := 0.0
	TimeSum := 0.0
	LastTime := int64(0)
	SizeSlice := make([]float64, 0, len(stats.S.Records))
	DeltaTimeSlice := make([]float64, 0, len(stats.S.Records)-1)
	for i, record := range stats.S.Records {
		SizeSlice = append(SizeSlice, float64(record.Size))
		SizeSum += float64(record.Size)
		TimeSum += float64(record.ConsumingTime)
		if i > 0 {
			DeltaTimeSlice = append(DeltaTimeSlice, float64((record.Time.UnixNano()-LastTime)/1e6))
		}
		LastTime = record.Time.UnixNano()
	}
	SizeMean, SizeStd := calculateMeanAndStd(SizeSlice)
	DeltaTimeMean, DeltaTimeStd := calculateMeanAndStd(DeltaTimeSlice)
	stats.S.SizeMean = SizeMean
	stats.S.SizeStd = SizeStd
	stats.S.DeltaTimeMean = DeltaTimeMean
	stats.S.DeltaTimeStd = DeltaTimeStd
	return SizeSum / TimeSum
}

func (stats *TsStats) CalculateStatisticsAndPerformance(id string) {
	if len(stats.S.Records) == 0 {
		return
	}
	SizeSum := 0.0
	TimeSum := 0.0
	LastTime := int64(0)
	SizeSlice := make([]float64, 0, len(stats.S.Records))
	DeltaTimeSlice := make([]float64, 0, len(stats.S.Records)-1)
	for i, record := range stats.S.Records {
		SizeSlice = append(SizeSlice, float64(record.Size))
		SizeSum += float64(record.Size)
		TimeSum += float64(record.ConsumingTime)
		if i > 0 {
			DeltaTimeSlice = append(DeltaTimeSlice, float64((record.Time.UnixNano()-LastTime)/1e6))
		}
		LastTime = record.Time.UnixNano()
	}
	SizeMean, SizeStd := calculateMeanAndStd(SizeSlice)
	DeltaTimeMean, DeltaTimeStd := calculateMeanAndStd(DeltaTimeSlice)
	stats.S.SizeMean = SizeMean
	stats.S.SizeStd = SizeStd
	stats.S.DeltaTimeMean = DeltaTimeMean
	stats.S.DeltaTimeStd = DeltaTimeStd
	if stats.P == nil {
		stats.P = &Performance{
			P: 0.0,
		}
	}
	stats.P.P = SizeSum / TimeSum
	// 针对封口了的 stats，则会将统计信息删除以释放空间
	if stats.Closed {
		stats.S.Records = nil
		fileLog.Info(0, "%s 的性能为 %f", id, stats.P.P)
	}
}

func calculateMeanAndStd(numbers []float64) (float64, float64) {
	if len(numbers) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, num := range numbers {
		sum += num
	}
	mean := sum / float64(len(numbers))
	vars := 0.0
	for _, num := range numbers {
		vars += (num - mean) * (num - mean)
	}
	vars /= float64(len(numbers))
	std := math.Sqrt(vars)
	return mean, std
}
