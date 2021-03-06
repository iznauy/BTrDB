package brain

import (
	"github.com/iznauy/BTrDB/brain/conf"
	ownLog "github.com/iznauy/BTrDB/brain/log"
	"github.com/iznauy/BTrDB/brain/stats"
	"github.com/iznauy/BTrDB/brain/tool"
	"github.com/iznauy/BTrDB/brain/types"
	"github.com/op/go-logging"
	"github.com/pborman/uuid"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var B *Brain

var (
	log     *logging.Logger
	fileLog *ownLog.Logger
)

var (
	appendTimes int64
	appendSpan  time.Duration
	appendMu    sync.RWMutex

	commitTimes int64
	commitSpan  time.Duration
	commitMu    sync.RWMutex

	createTimes int64
	createSpan  time.Duration
	createMu    sync.RWMutex
)

func init() {
	B = &Brain{
		SystemStats:      stats.NewSystemStats(),
		ApplicationStats: stats.NewApplicationStats(),
		handlers:         map[types.EventType][]types.EventHandler{},
	}
	log = logging.MustGetLogger("log")
	fileLog = ownLog.GetLogger()
	go BroadcastStatistics()
}

type Brain struct {
	SystemStats      *stats.SystemStats
	ApplicationStats *stats.ApplicationStats
	DecisionTimes    int64

	handlers map[types.EventType][]types.EventHandler
}

func BroadcastStatistics() {
	for {
		time.Sleep(5 * time.Second) // 每隔五秒输出一次统计信息

		appendMu.RLock()
		if appendTimes > 0 {
			log.Infof("append 事件共触发了%v次，总耗时%vms，平均耗时%v微秒.", appendTimes, appendSpan.Milliseconds(), appendSpan.Microseconds()*1.0/appendTimes)
		} else {
			log.Info("append 事件尚未触发")
		}
		appendMu.RUnlock()

		createMu.RLock()
		if createTimes > 0 {
			log.Infof("create 事件共触发了%v次，总耗时%vms，平均耗时%v微秒", createTimes, createSpan.Milliseconds(), createSpan.Microseconds()*1.0/createTimes)
		} else {
			log.Info("create 事件尚未触发")
		}
		createMu.RUnlock()

		commitMu.RLock()
		if commitTimes > 0 {
			log.Infof("commit 事件共触发了%v次，总耗时%vms，平均耗时%v微秒", commitTimes, commitSpan.Milliseconds(), commitSpan.Microseconds()*1.0/commitTimes)
		} else {
			log.Info("commit 事件尚未触发")
		}
		commitMu.RUnlock()

		B.SystemStats.BufferMutex.RLock()
		buffer := B.SystemStats.Buffer
		log.Infof("buffer 统计信息：总使用的空间 = %v，总分配的空间 = %v, 内存中驻留的时间序列 = %v", buffer.TotalUsedSpace, buffer.TotalAllocatedSpace, buffer.TimeSeriesInMemory)
		B.SystemStats.BufferMutex.RUnlock()
	}
}

func (b *Brain) Emit(e *types.Event) {
	if e == nil {
		return
	}
	if e.Type != types.WriteRequest && e.Type != types.CommitBuffer && e.Type != types.CreateBuffer {
		return
	}
	//defer func() func() {
	//	begin := time.Now()
	//	return func() {
	//		localSpan := time.Now().Sub(begin)
	//		if e.Type == types.AppendBuffer {
	//			appendMu.Lock()
	//			appendSpan += localSpan
	//			appendTimes += 1
	//			appendMu.Unlock()
	//		}
	//		if e.Type == types.CommitBuffer {
	//			commitMu.Lock()
	//			commitSpan += localSpan
	//			commitTimes += 1
	//			commitMu.Unlock()
	//		}
	//		if e.Type == types.CreateBuffer {
	//			createMu.Lock()
	//			createSpan += localSpan
	//			createTimes += 1
	//			createMu.Unlock()
	//		}
	//	}
	//} ()()
	//eventNumber := rand.Int()
	//fmt.Printf("event: %v, id = %d 开始处理\n", e, eventNumber)
	for _, h := range b.handlers[e.Type] {
		if !h.Process(e) {
			break
		}
	}
	//fmt.Printf("event: %v, id = %d 处理完毕\n", e, eventNumber)
}

func (b *Brain) GetReadAndWriteLimiter() (int64, int64) {
	return 1000000000, 1000000000
}

func (b *Brain) GetBufferMaxSizeAndCommitInterval(id uuid.UUID) (uint64, uint64) {
	decisionId := rand.Int()
	bufferSize, commitInterval := b.getBufferMaxSizeAndCommitInterval(id, decisionId)
	ts := b.SystemStats.GetTs(tool.UUIDToMapKey(id))
	if ts.StatsList.Size != 0 {
		tsStats := ts.StatsList.Tail.Data
		if tsStats.Period == 0 {
			tsStats.A = &stats.Action{
				Action:         types.BufferSize,
				BufferSize:     bufferSize,
				CommitInterval: commitInterval,
			}
			tsStats.CalculateStatisticsAndPerformance(id.String())
			ts.StatsList.Append(stats.NewTsStats())
		}
	}
	return bufferSize, commitInterval
}

func (b *Brain) getBufferMaxSizeAndCommitInterval(id uuid.UUID, decisionId int) (uint64, uint64) {
	fileLog.Info(decisionId, "关于时间序列 %s 的决策", id.String())
	ts := b.SystemStats.GetTs(tool.UUIDToMapKey(id))
	if ts.LastCommitTime == nil { // 新时间序列只能采用平均法进行第一次决策！
		ts.BufferSize = 1000 + uint64(rand.Int()%9000)
		ts.CommitInterval = 2000 + uint64(rand.Int()%18000)
		now := time.Now()
		ts.LastCommitTime = &now
		fileLog.Info(decisionId, "新时间序列 %s 使用随机 bufferSize 和 commitInterval。bufferSize = %d, commitInterval = %d", id.String(), ts.BufferSize, ts.CommitInterval)
		return ts.BufferSize, ts.CommitInterval
	}
	// 多个周期后才会变换
	tail := ts.StatsList.Tail.Data
	if tail.Period != 0 {
		fileLog.Info(decisionId, "时间序列 %s 还有 %d 周期进行下次决策", id.String(), tail.Period)
		return ts.BufferSize, ts.CommitInterval
	}
	// 一定概率采用随机策略
	if b.makeRandomDecision() {
		ts.BufferSize = 1000 + uint64(rand.Int()%9000)
		ts.CommitInterval = 2000 + uint64(rand.Int()%18000)
		fileLog.Info(decisionId, "时间序列 %s 使用随机 bufferSize 和 commitInterval。bufferSize = %d, commitInterval = %d", id.String(), ts.BufferSize, ts.CommitInterval)
		return ts.BufferSize, ts.CommitInterval
	}
	// 不采用随机策略的话，先是从各个时间序列中随机选出50个，然后找出最相近的4个时间序列，随后附加上当前时间序列，最后再根据其当前性能进行排序
	greatTs := b.findGreatestTsForBufferSize(ts, decisionId)
	if greatTs == nil { // 有可能当前所有的时间序列都没有先验知识，那我们还是只能采用随机策略
		ts.BufferSize = 1000 + uint64(rand.Int()%9000)
		ts.CommitInterval = 2000 + uint64(rand.Int()%18000)
		fileLog.Info(decisionId, "由于系统中未采样到可以学习的时间序列，因此时间序列 %s 使用随机 bufferSize 和 commitInterval。bufferSize = %d, commitInterval = %d", id.String(), ts.BufferSize, ts.CommitInterval)
		return ts.BufferSize, ts.CommitInterval
	}
	tsNode := greatTs.StatsList.Tail
	for {
		if tsNode == nil {
			panic("That should not happened")
		}
		if tsNode.Data.P == nil {
			tsNode = tsNode.Prev
			continue
		}
		action := tsNode.Data.A
		if action.Action != types.BufferSize {
			tsNode = tsNode.Prev
			continue
		}
		ts.BufferSize = action.BufferSize
		ts.CommitInterval = action.CommitInterval
		fileLog.Info(decisionId, "计算 bufferSize 和 commitInterval 时与 %s 最相近的时间序列为 %s，bufferSize = %d，commitInterval = %d", id.String(), uuid.UUID(greatTs.ID[:]).String(), ts.BufferSize, ts.CommitInterval)
		return ts.BufferSize, ts.CommitInterval
	}
	return ts.BufferSize, ts.CommitInterval
}

func (b *Brain) GetBufferType(id uuid.UUID) types.BufferType {
	return types.Slice
}

func (b *Brain) GetCacheMaxSize() uint64 {
	return 125000
}

func (b *Brain) GetKAndVForNewTimeSeries(id uuid.UUID) (K uint16, V uint32) {
	//K, V = b.getKAndVForNewTimeSeries(id)
	return 64, 1024
}

func (b *Brain) getKAndVForNewTimeSeries(id uuid.UUID) (K uint16, V uint32) {
	decisionId := rand.Int()
	ts := b.SystemStats.GetTs(tool.UUIDToMapKey(id))
	if b.makeRandomDecision() {
		K, V = randomGetKAndV()
		ts.K = K
		ts.V = V
		fileLog.Info(decisionId, "%s 计算KV时采用了随机策略，K = %d, V = %d", id.String(), K, V)
		return ts.K, ts.V
	}
	greatTs := b.findGreatestTsForKAndV(ts)
	if greatTs == nil {
		K, V = randomGetKAndV()
		ts.K = K
		ts.V = V
		fileLog.Info(decisionId, "由于目前系统中未采样到可以学习的时间序列，因此 %s 计算KV时采用了随机策略，K = %d，V = %d", id.String(), K, V)
		return ts.K, ts.V
	}
	ts.K = greatTs.K
	ts.V = greatTs.V
	fileLog.Info(decisionId, "计算KV时与 %s 最相近的时间序列为 %s，K = %d，V = %d", id.String(), uuid.UUID(greatTs.ID[:]).String(), K, V)
	return ts.K, ts.V
}

func randomGetKAndV() (K uint16, V uint32) {
	return 64, 1024
}

func (b *Brain) RegisterEventHandler(tp types.EventType, handler types.EventHandler) {
	handlerMap := b.handlers
	handlers, ok := handlerMap[tp]
	if !ok {
		handlers = make([]types.EventHandler, 0, 1)
	}
	handlers = append(handlers, handler)
	handlerMap[tp] = handlers
}

func (b *Brain) makeRandomDecision() bool {
	prob := conf.Alpha * math.Pow(conf.Beta, float64(atomic.LoadInt64(&b.DecisionTimes)))
	atomic.AddInt64(&b.DecisionTimes, 1)
	return rand.Float64() < prob
}

func (b *Brain) findGreatestTsForKAndV(ts *stats.Ts) *stats.Ts {
	//decisionNumber := rand.Int()
	//fmt.Printf("findGreatestTsForKAndV, decisionNumber = %d 开始处理\n", decisionNumber)
	//
	//defer func() {
	//	fmt.Printf("findGreatestTsForKAndV, decisionNumber = %d 处理结束\n", decisionNumber)
	//}()

	randomSampleTss := b.SystemStats.RandomSampleTs(conf.SampleCount)
	randomSampleTsMap := make(map[[16]byte]*stats.Ts, len(randomSampleTss))
	distances := make([]*Distance, 0, len(randomSampleTss))
	tsStats := ts.StatsList.Tail.Data
	tsStats.CalculateStatisticsAndGetP()
	for _, sampleTs := range randomSampleTss {
		randomSampleTsMap[sampleTs.ID] = sampleTs
		minDistance := math.MaxFloat64
		sampleTs.StatsList.Foreach(func(stats *stats.TsStats) {
			if stats.P == nil {
				return
			}
			distance := stats.S.Distance(tsStats.S)
			if minDistance > distance {
				minDistance = distance
			}
		})
		if minDistance != math.MaxFloat64 {
			distances = append(distances, &Distance{
				distance: minDistance,
				id:       sampleTs.ID,
			})
		}
	}
	if len(distances) == 0 {
		return nil
	}
	sort.Slice(distances, func(i, j int) bool {
		return distances[i].distance < distances[j].distance
	})
	if len(distances) > conf.DecisionSetCount {
		distances = distances[:conf.DecisionSetCount]
	}
	greatestTs := randomSampleTsMap[distances[0].id]
	greatestP := greatestTs.StatsList.Tail.Prev.Data.P.P
	for _, distance := range distances {
		id := distance.id
		currentTs := randomSampleTsMap[id]
		if currentTs.StatsList.Size < 2 {
			continue
		}
		currentP := randomSampleTsMap[id].StatsList.Tail.Prev.Data.P.P
		if currentP > greatestP {
			greatestTs = currentTs
		}
	}
	return greatestTs
}

func (b *Brain) findGreatestTsForBufferSize(ts *stats.Ts, decisionId int) *stats.Ts {
	randomSampleTss := b.SystemStats.RandomSampleTs(conf.SampleCount)
	randomSampleTsMap := make(map[[16]byte]*stats.Ts, len(randomSampleTss))
	distances := make([]*Distance, 0, len(randomSampleTss))
	tsStats := ts.StatsList.Tail.Data // 最近的一次 buffer 生命周期中时间序列的统计数据，仅用于计算相似度
	P := tsStats.CalculateStatisticsAndGetP()
	fileLog.Info(decisionId, "当前时间序列 %s 的状态为 %s", uuid.UUID(ts.ID[:]).String(), tsStats.S.String())
	for _, sampleTs := range randomSampleTss {
		randomSampleTsMap[sampleTs.ID] = sampleTs
		var bestStats *stats.TsStats
		minDistance := math.MaxFloat64
		sampleTs.StatsList.Foreach(func(stats *stats.TsStats) {
			if stats.P == nil {
				return
			}
			distance := stats.S.Distance(tsStats.S)
			distance += (stats.P.P * conf.PerformanceBalanceFactor - P * conf.PerformanceBalanceFactor) *
				(stats.P.P * conf.PerformanceBalanceFactor - P * conf.PerformanceBalanceFactor)
			if minDistance > distance {
				minDistance = distance
				bestStats = stats
			}
		})
		if minDistance != math.MaxFloat64 { // 只有有过提交记录的时间序列才会被匹配到
			distances = append(distances, &Distance{
				distance: minDistance,
				id:       sampleTs.ID,
			})
			fileLog.Info(decisionId, "时间序列 %s 与当前时间序列 %s 的距离为 %f, 其状态为 %s", uuid.UUID(sampleTs.ID[:]).String(), uuid.UUID(ts.ID[:]).String(), minDistance,
				bestStats.S.String())
		}
	}
	if len(distances) == 0 { // 别的时间序列都还没提交过，你自求多福吧！
		return nil
	}
	sort.Slice(distances, func(i, j int) bool {
		return distances[i].distance < distances[j].distance
	})
	if len(distances) > conf.DecisionSetCount {
		distances = distances[:conf.DecisionSetCount]
	}
	var greatestTs *stats.Ts = nil
	var greatestP = 0.0

	if ts.StatsList.Size > 1 { // 并不是第一个提交周期
		greatestTs = ts
		greatestP = ts.StatsList.Tail.Prev.Data.P.P
	}
	for _, distance := range distances {
		id := distance.id
		currentTs := randomSampleTsMap[id]
		if currentTs.StatsList.Size < 2 {
			continue
		}
		currentP := randomSampleTsMap[id].StatsList.Tail.Prev.Data.P.P
		fileLog.Info(decisionId, "比较相似的时间序列 %s 的性能为：%f", uuid.UUID(currentTs.ID[:]).String(), currentP)
		if greatestTs == nil || currentP > greatestP {
			greatestTs = currentTs
			greatestP = currentP
		}
	}
	if greatestTs != nil {
		fileLog.Info(decisionId, "相似的时间序列中，性能最好的为 %s, 其性能为 %f", uuid.UUID(greatestTs.ID[:]).String(), greatestP)
	} else {
		fileLog.Info(decisionId, "系统内数据过少，没有足够的数据找到最相近的时间序列")
	}
	return greatestTs
}

type Distance struct {
	distance float64
	id       [16]byte
}
