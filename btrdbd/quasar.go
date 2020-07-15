package btrdbd

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/iznauy/BTrDB/inter/bstore"
	"github.com/iznauy/BTrDB/qtree"
	"github.com/op/go-logging"
	"github.com/pborman/uuid"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

type openTree struct { // 缓冲区
	store []qtree.Record
	id    uuid.UUID
	sigEC chan bool // 提前提交的时候往这个里面发个消息，让当次的定期任务不再执行
}

const MinimumTime = -(16 << 56)
const MaximumTime = (48 << 56)
const LatestGeneration = bstore.LatestGeneration

type Quasar struct {
	cfg QuasarConfig
	bs  *bstore.BlockStore

	//Transaction coalescence
	globlock  sync.Mutex // 全局锁
	treelocks map[[16]byte]*sync.Mutex // 🌲锁
	openTrees map[[16]byte]*openTree  // 每个时间序列对应自己的缓冲区
}

func newOpenTree(id uuid.UUID) *openTree {
	return &openTree{
		id: id,
	}
}

type QuasarConfig struct {
	//Measured in the number of datablocks
	//So 1000 is 8 MB cache
	DatablockCacheSize uint64

	//This enables the grouping of value inserts
	//with a commit every Interval millis
	//If the number of stored values exceeds
	//EarlyTrip
	TransactionCoalesceEnable    bool // 是否开启 TransactionCoalesce
	TransactionCoalesceInterval  uint64 // 必须要提交的时间间隔
	TransactionCoalesceEarlyTrip uint64 // 必须要提交的点的数量

	Params map[string]string
}

// 判断是否有数据在 buffer 之中
// Return true if there are uncommited results to be written to disk
// Should only be used during shutdown as it hogs the glock
func (q *Quasar) IsPending() bool {
	isPend := false
	q.globlock.Lock()
	for uuid, ot := range q.openTrees {
		q.treelocks[uuid].Lock()
		if len(ot.store) != 0 {
			isPend = true
			q.treelocks[uuid].Unlock()
			break
		}
		q.treelocks[uuid].Unlock()
	}
	q.globlock.Unlock()
	return isPend
}

func NewQuasar(cfg *QuasarConfig) (*Quasar, error) {
	bs, err := bstore.NewBlockStore(cfg.Params)
	if err != nil {
		return nil, err
	}
	rv := &Quasar{
		cfg:       *cfg,
		bs:        bs,
		openTrees: make(map[[16]byte]*openTree, 128),
		treelocks: make(map[[16]byte]*sync.Mutex, 128),
	}
	return rv, nil
}

// 获取某个树的缓冲区及其对应的锁
func (q *Quasar) getTree(id uuid.UUID) (*openTree, *sync.Mutex) {
	mk := bstore.UUIDToMapKey(id)
	q.globlock.Lock()
	ot, ok := q.openTrees[mk]
	if !ok {
		ot := newOpenTree(id)
		mtx := &sync.Mutex{}
		q.openTrees[mk] = ot
		q.treelocks[mk] = mtx
		q.globlock.Unlock()
		return ot, mtx
	}
	mtx, ok := q.treelocks[mk]
	if !ok {
		log.Panicf("This should not happen")
	}
	q.globlock.Unlock()
	return ot, mtx
}

// 提交缓冲区中的数据
func (t *openTree) commit(q *Quasar) {
	if len(t.store) == 0 {
		//This might happen with a race in the timeout commit
		fmt.Println("no store in commit")
		return
	}
	tr, err := qtree.NewWriteQTree(q.bs, t.id)
	if err != nil {
		log.Panic(err)
	}
	if err := tr.InsertValues(t.store); err != nil {
		log.Error("BAD INSERT: ", err)
	}
	tr.Commit()
	t.store = nil
}

// 插入数据点，如果没达到 buffer 限制，将数据插入到 buffer，否则将会提前刷新数据
func (q *Quasar) InsertValues(id uuid.UUID, r []qtree.Record) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("BAD INSERT: ", r)
		}
	}()
	tr, mtx := q.getTree(id)
	mtx.Lock()
	if tr == nil {
		log.Panicf("This should not happen")
	}
	if tr.store == nil {
		//Empty store
		tr.store = make([]qtree.Record, 0, len(r)*2)
		tr.sigEC = make(chan bool, 1)
		//Also spawn the coalesce timeout goroutine
		go func(abrt chan bool) {
			tmt := time.After(time.Duration((q.cfg.TransactionCoalesceInterval >> 1) + rand.Uint64() % q.cfg.TransactionCoalesceInterval) * time.Millisecond)
			select {
			case <-tmt:
				//do coalesce
				mtx.Lock()
				//In case we early tripped between waiting for lock and getting it, commit will return ok
				//log.Debug("Coalesce timeout %v", id.String())
				tr.commit(q)
				mtx.Unlock()
			case <-abrt:
				return
			}
		}(tr.sigEC)
	}
	tr.store = append(tr.store, r...)
	if uint64(len(tr.store)) >= q.cfg.TransactionCoalesceEarlyTrip {
		tr.sigEC <- true
		log.Debug("Coalesce early trip %v", id.String())
		tr.commit(q)
	}
	mtx.Unlock()
}

// 强行刷新数据
func (q *Quasar) Flush(id uuid.UUID) error {
	tr, mtx := q.getTree(id)
	mtx.Lock()
	if len(tr.store) != 0 {
		tr.sigEC <- true
		tr.commit(q)
		fmt.Printf("Commit done %+v\n", id)
	} else {
		fmt.Printf("no store\n")
	}
	mtx.Unlock()
	return nil
}

// 读取某个版本下某个时间区间的所有数据
//These functions are the API. TODO add all the bounds checking on PW, and sanity on start/end
func (q *Quasar) QueryValues(id uuid.UUID, start int64, end int64, gen uint64) ([]qtree.Record, uint64, error) {
	tr, err := qtree.NewReadQTree(q.bs, id, gen) // 从 MongoDB 中加载超级块，超级块包括版本信息，根节点地址 root，unlinked 字段
	if err != nil {
		return nil, 0, err
	}
	rv, err := tr.ReadStandardValuesBlock(start, end)
	return rv, tr.Generation(), err
}

// 提供一个读取时间区间 chan
func (q *Quasar) QueryValuesStream(id uuid.UUID, start int64, end int64, gen uint64) (chan qtree.Record, chan error, uint64) {
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, nil, 0
	}
	recordc := make(chan qtree.Record)
	errc := make(chan error)
	go tr.ReadStandardValuesCI(recordc, errc, start, end)
	return recordc, errc, tr.Generation()
}

// 读取某个时间区间的统计信息
func (q *Quasar) QueryStatisticalValues(id uuid.UUID, start int64, end int64,
	gen uint64, pointwidth uint8) ([]qtree.StatRecord, uint64, error) {
	//fmt.Printf("QSV0 s=%v e=%v pw=%v\n", start, end, pointwidth)
	start &^= ((1 << pointwidth) - 1) // 清空低位
	end &^= ((1 << pointwidth) - 1)
	end -= 1
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, 0, err
	}
	rv, err := tr.QueryStatisticalValuesBlock(start, end, pointwidth)
	if err != nil {
		return nil, 0, err
	}
	return rv, tr.Generation(), nil
}
func (q *Quasar) QueryStatisticalValuesStream(id uuid.UUID, start int64, end int64,
	gen uint64, pointwidth uint8) (chan qtree.StatRecord, chan error, uint64) {
	fmt.Printf("QSV1 s=%v e=%v pw=%v\n", start, end, pointwidth)
	start &^= ((1 << pointwidth) - 1)
	end &^= ((1 << pointwidth) - 1)
	end -= 1
	rvv := make(chan qtree.StatRecord, 1024)
	rve := make(chan error)
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, nil, 0
	}
	go tr.QueryStatisticalValues(rvv, rve, start, end, pointwidth)
	return rvv, rve, tr.Generation()
}

func (q *Quasar) QueryWindow(id uuid.UUID, start int64, end int64,
	gen uint64, width uint64, depth uint8) (chan qtree.StatRecord, uint64) {
	rvv := make(chan qtree.StatRecord, 1024)
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, 0
	}
	go tr.QueryWindow(start, end, width, depth, rvv)
	return rvv, tr.Generation()
}

// 获取最新的版本号信息
func (q *Quasar) QueryGeneration(id uuid.UUID) (uint64, error) {
	sb := q.bs.LoadSuperblock(id, bstore.LatestGeneration)
	if sb == nil {
		return 0, qtree.ErrNoSuchStream
	}
	return sb.Gen(), nil
}

// 查询最接近的数据点
func (q *Quasar) QueryNearestValue(id uuid.UUID, time int64, backwards bool, gen uint64) (qtree.Record, uint64, error) {
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return qtree.Record{}, 0, err
	}
	rv, err := tr.FindNearestValue(time, backwards)
	return rv, tr.Generation(), err
}

type ChangedRange struct {
	Start int64
	End   int64
}

//Resolution is how far down the tree to go when working out which blocks have changed. Higher resolutions are faster
//but will give you back coarser results.
// 读取某两个版本之间变化的时间区间
func (q *Quasar) QueryChangedRanges(id uuid.UUID, startgen uint64, endgen uint64, resolution uint8) ([]ChangedRange, uint64, error) {
	//0 is a reserved generation, so is 1, which means "before first"
	if startgen == 0 {
		startgen = 1
	}
	tr, err := qtree.NewReadQTree(q.bs, id, endgen)
	if err != nil {
		log.Debug("Error on QCR open tree")
		return nil, 0, err
	}
	rv := make([]ChangedRange, 0, 1024)
	rch := tr.FindChangedSince(startgen, resolution)
	var lr *ChangedRange = nil
	for {

		select {
		case cr, ok := <-rch:
			if !ok {
				//This is the end.
				//Do we have an unsaved LR?
				if lr != nil {
					rv = append(rv, *lr)
				}
				return rv, tr.Generation(), nil
			}
			if !cr.Valid {
				log.Panicf("Didn't think this could happen")
			}
			//Coalesce
			if lr != nil && cr.Start == lr.End {
				lr.End = cr.End
			} else {
				if lr != nil {
					rv = append(rv, *lr)
				}
				lr = &ChangedRange{Start: cr.Start, End: cr.End}
			}
		}
	}
	return rv, tr.Generation(), nil
}

// 删除某个时间区间，这边涉及到写的问题
func (q *Quasar) DeleteRange(id uuid.UUID, start int64, end int64) error {
	tr, mtx := q.getTree(id)
	mtx.Lock()
	if len(tr.store) != 0 {
		tr.sigEC <- true
		tr.commit(q) // 执行删除操作的时候，如果还有没刷新的数据，先刷新数据
	}
	wtr, err := qtree.NewWriteQTree(q.bs, id)
	if err != nil {
		log.Panic(err)
	}
	err = wtr.DeleteRange(start, end)
	if err != nil {
		log.Panic(err)
	}
	wtr.Commit()
	mtx.Unlock()
	return nil
}
