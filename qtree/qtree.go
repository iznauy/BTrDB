package qtree

import (
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"math"
	"sort"
	"time"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

//but we still have support for dates < 1970

var ErrNoSuchStream = errors.New("no such stream")
var ErrNotLeafNode = errors.New("not a leaf node")
var ErrImmutableTree = errors.New("tree is immutable")
var ErrIdxNotFound = errors.New("index not found")
var ErrNoSuchPoint = errors.New("no such point")
var ErrBadInsert = errors.New("bad insert")
var ErrBadDelete = errors.New("bad delete")

//It is important to note that if backwards is true, then time is exclusive. So if
//a record exists with t=80 and t=100, and you query with t=100, backwards=true, you will get the t=80
//record. For forwards, time is inclusive.
func (n *QTreeNode) FindNearestValue(time int64, backwards bool) (Record, error) {
	if n.isLeaf {

		if n.vector_block.Len == 0 {
			log.Panicf("Not expecting this")
		}
		idx := -1
		for i := 0; i < int(n.vector_block.Len); i++ {
			if n.vector_block.Time[i] >= time {
				if !backwards {
					idx = i
				}
				break
			}
			if backwards {
				idx = i
			}
		}
		if idx == -1 {
			//If backwards that means first point is >
			//If forwards that means last point is <
			return Record{}, ErrNoSuchPoint
		}
		return Record{
			Time: n.vector_block.Time[idx],
			Val:  n.vector_block.Value[idx],
		}, nil
	} else {
		//We need to find which child with nonzero count is the best to satisfy the claim.
		idx := -1
		for i := 0; i < n.tr.GetKFactor(); i++ {
			if n.core_block.Count[i] == 0 {
				continue
			}
			if n.ChildStartTime(uint16(i)) >= time {
				if !backwards {
					idx = i
				}
				break
			}
			if backwards {
				idx = i
			}
		}
		if idx == -1 {
			return Record{}, ErrNoSuchPoint
		}
		//for backwards, idx points to the containing window
		//for forwards, idx points to the window after the containing window

		val, err := n.Child(uint16(idx)).FindNearestValue(time, backwards)

		//For both, we also need the window before this point
		// TODO 感觉这儿有 bug，在 backward = false 以及 backward = true 但是 n.core_block.Count[idx-1] = 0 时候会出错
		if idx != 0 && n.core_block.Count[idx-1] != 0 { //The block containing the time is not empty

			//So if we are going forward, we need to do two queries, the window that CONTAINS the time, and the window
			//that FOLLOWS the time, because its possible for all the data points in the CONTAINS window to fall before
			//the time. For backwards we have the same thing but VAL above is the CONTAINS window, and we need to check
			//the BEFORE window
			other, oerr := n.Child(uint16(idx-1)).FindNearestValue(time, backwards)
			if oerr == ErrNoSuchPoint {
				//Oh well the standard window is the only option
				return val, err
			}

			if backwards {
				//The val is best
				if err == nil {
					return val, nil
				} else {
					return other, oerr
				}
			} else { //Other is best
				if oerr == nil {
					return other, nil
				} else {
					return val, err
				}
			}
		}

		return val, err
	}
}

func (tr *QTree) FindChangedSinceSlice(gen uint64, resolution uint8) []ChangedRange {
	if tr.root == nil {
		return make([]ChangedRange, 0)
	}
	rv := make([]ChangedRange, 0, 1024)
	rch := tr.FindChangedSince(gen, resolution)
	var lr ChangedRange = ChangedRange{}
	for {
		select {
		case cr, ok := <-rch:
			if !ok {
				//This is the end.
				//Do we have an unsaved LR?
				if lr.Valid {
					rv = append(rv, lr)
				}
				return rv
			}
			if !cr.Valid {
				log.Panicf("Didn't think this could happen")
			}
			//Coalesce
			if lr.Valid && cr.Start == lr.End {
				lr.End = cr.End
			} else {
				if lr.Valid {
					rv = append(rv, lr)
				}
				lr = cr
			}
		}
	}
	return rv
}

func (tr *QTree) FindChangedSince(gen uint64, resolution uint8) chan ChangedRange {
	rv := make(chan ChangedRange, 1024)
	go func() {
		if tr.root == nil {
			close(rv)
			return
		}
		cr := tr.root.FindChangedSince(gen, rv, resolution)
		if cr.Valid {
			rv <- cr
		}
		close(rv)
	}()
	return rv
}

func (n *QTreeNode) DeleteRange(start int64, end int64) *QTreeNode {
	if n.isLeaf {
		widx, ridx := 0, 0
		//First check if this operation deletes all the entries or only some
		if n.vector_block.Len == 0 {
			log.Panicf("This shouldn't happen")
		}
		if start <= n.vector_block.Time[0] && end > n.vector_block.Time[n.vector_block.Len-1] { // 范围包含了这整个叶子结点，直接返回空节点作为新节点
			return nil
		}
		//Otherwise we need to copy the parts that still exist
		log.Debug("Calling uppatch loc1")
		newn, err := n.AssertNewUpPatch()
		if err != nil {
			log.Panicf("Could not up patch: %v", err)
		}
		n = newn
		for ridx < int(n.vector_block.Len) { // 把不需要被删除的数据往前挪，然后修改 vector_block 中的 len 字段
			//if n.vector_block.
			if n.vector_block.Time[ridx] < start || n.vector_block.Time[ridx] >= end {
				n.vector_block.Time[widx] = n.vector_block.Time[ridx]
				n.vector_block.Value[widx] = n.vector_block.Value[ridx]
				widx++
			}
			ridx++
		}
		n.vector_block.Len = uint16(widx)
		return n
	} else {
		if start <= n.StartTime() && end > n.EndTime() { // 范围包含了这整个子树，直接返回空节点作为新节点
			//This node is being deleted in its entirety. As we are no longer using the dereferences, we can
			//prune the whole branch up here. Note that this _does_ leak references for all the children, but
			//we are no longer using them
			return nil
		}

		//We have at least one reading somewhere in here not being deleted
		sb := n.ClampBucket(start)
		eb := n.ClampBucket(end)

		//Check if there are nodes fully outside the range being deleted
		othernodes := false
		for i := uint16(0); i < sb; i++ {
			if n.core_block.Addr[i] != 0 {
				othernodes = true
				break
			}
		}
		for i := eb + 1; i < uint16(n.tr.GetKFactor()); i++ {
			if n.core_block.Addr[i] != 0 {
				othernodes = true
				break
			}
		}

		//Replace our children
		newchildren := make([]*QTreeNode, 64)
		nonnull := false
		for i := sb; i <= eb; i++ {
			ch := n.Child(i)
			if ch != nil {
				newchildren[i] = ch.DeleteRange(start, end)
				if newchildren[i] != nil {
					nonnull = true
					//The child might have done the uppatch
					if newchildren[i].Parent() != n {
						n = newchildren[i].Parent()
					}
				}
			}
		}

		if !nonnull && !othernodes { // 所有的子节点都删没了（范围内所有节点都被删了，不在范围内的阶段原本也都是空的）
			return nil
		} else {
			//This node is not completely empty
			newn, err := n.AssertNewUpPatch()
			if err != nil {
				log.Panicf("Could not up patch: %v", err)
			}
			n = newn
			//nil children unfref'd themselves, so we should be ok with just marking them as nil
			for i := sb; i <= eb; i++ {
				n.SetChild(i, newchildren[i])
			}
			return n
		}
	}
}

//TODO: consider deletes. I think that it will require checking if the generation of a core node is higher than all it's non-nil
//children. This implies that one or more children got deleted entirely, and then the node must report its entire time range as changed.
//it's not possible for a child to have an equal generation to the parent AND another node got deleted, as deletes and inserts do not
//get batched together. Also, if we know that a generation always corresponds to a contiguous range deletion (i.e we don't coalesce
//deletes) then we know that we couldn't have got a partial delete that bumped up a gen and masked another full delete.
//NOTE: We should return changes SINCE a generation, so strictly greater than.
func (n *QTreeNode) FindChangedSince(gen uint64, rchan chan ChangedRange, resolution uint8) ChangedRange {
	if n.isLeaf {
		if n.vector_block.Generation <= gen {
			//This can happen if the root is a leaf. Not sure if we allow that or not
			log.Error("Should not have executed here1")
			return ChangedRange{} //Not valid
		}
		//This is acceptable, the parent had no way of knowing we were a leaf
		return ChangedRange{true, n.StartTime(), n.EndTime()}
	} else {
		if n.core_block.Generation < gen {
			//Parent should not have called us, it knows our generation
			log.Error("Should not have executed here2")
			return ChangedRange{} //Not valid
		}
		/*if n.PointWidth() <= resolution {
			//Parent should not have called us, it knows our pointwidth
			log.Error("Should not have executed here3")
			return ChangedRange{true, n.StartTime(), n.EndTime()}
		}*/
		cr := ChangedRange{}
		maxchild := uint64(0)
		for k := 0; k < n.tr.GetKFactor(); k++ {
			if n.core_block.CGeneration[k] > maxchild {
				maxchild = n.core_block.CGeneration[k]
			}
		}
		if maxchild > n.Generation() {
			log.Panicf("Children are older than parent (this is bad) here: %s", n.TreePath())
		}

		norecurse := n.PointWidth() <= resolution
		for k := 0; k < n.tr.GetKFactor(); k++ {
			if n.core_block.CGeneration[k] > gen {
				if n.core_block.Addr[k] == 0 || norecurse {
					//A whole child was deleted here or we don't want to recurse further
					cstart := n.ChildStartTime(uint16(k))
					cend := n.ChildEndTime(uint16(k))
					if cr.Valid {
						if cstart == cr.End {
							cr.End = cend
						} else {
							rchan <- cr
							cr = ChangedRange{End: cend, Start: cstart, Valid: true}
						}
					} else {
						cr = ChangedRange{End: cend, Start: cstart, Valid: true}
					}
				} else {
					//We have a child, we need to recurse, and it has a worthy generation:
					rcr := n.Child(uint16(k)).FindChangedSince(gen, rchan, resolution)
					if rcr.Valid {
						if cr.Valid {
							if rcr.Start == cr.End {
								//If the changed range is connected, just extend what we have
								cr.End = rcr.End
							} else {
								//Send out the prev. changed range
								rchan <- cr
								cr = rcr
							}
						} else {
							cr = rcr
						}
					}
				}
			}
		}
		//Note that we don't get 100% coalescence. The receiver on rchan should also check for coalescence.
		//we just do a bit to reduce traffic on the channel. One case is if we have two disjoint ranges in a
		//core, and the first is at the start. We send it on rchan even if it might be adjacent to the prev
		//sibling
		return cr //Which might be invalid if we got none from children (all islanded)
	}
	return ChangedRange{}
}

func (n *QTreeNode) HasChild(i uint16) bool {
	if n.isLeaf {
		log.Panicf("Child of leaf?")
	}
	if n.core_block.Addr[i] == 0 {
		return false
	}
	return true
}

// 访问某个🌲的子节点，如果节点在缓存就读缓存，不在缓存就读数据
func (n *QTreeNode) Child(i uint16) *QTreeNode {
	//log.Debug("Child %v called on %v",i, n.TreePath())
	if n.isLeaf {
		log.Panicf("Child of leaf?")
	}
	if n.core_block.Addr[i] == 0 {
		return nil
	}
	if n.child_cache[i] != nil {
		return n.child_cache[i]
	}

	child, err := n.tr.LoadNode(n.core_block.Addr[i],
		n.core_block.CGeneration[i], n.ChildPW(), n.ChildStartTime(i))
	if err != nil {
		log.Debug("We are at %v", n.TreePath())
		log.Debug("We were trying to load child %v", i)
		log.Debug("With address %v", n.core_block.Addr[i])
		log.Panicf("%v", err)
	}
	child.parent = n
	n.child_cache[i] = child
	return child
}

//Like Child() but creates the node if it doesn't exist
func (n *QTreeNode) wchild(i uint16, isVector bool) *QTreeNode {
	if n.isLeaf {
		log.Panicf("Child of leaf?")
	}
	if n.tr.gen == nil {
		log.Panicf("Cannot use WChild on read only tree")
	}
	if n.PointWidth() == 0 {
		log.Panicf("Already at the bottom of the tree!")
	} else {
		//	log.Debug("ok %d", n.PointWidth())
	}
	if n.core_block.Addr[i] == 0 {
		//log.Debug("no existing child. spawning pw(%v)[%v] vector=%v", n.PointWidth(),i,isVector)
		var newn *QTreeNode
		var err error
		//log.Debug("child window is s=%v",n.ChildStartTime(i))
		if isVector {
			newn, err = n.tr.NewVectorNode(n.ChildStartTime(i), n.ChildPW())
		} else {
			newn, err = n.tr.NewCoreNode(n.ChildStartTime(i), n.ChildPW())
		}
		if err != nil {
			log.Panicf("%v", err)
		}
		newn.parent = n
		n.child_cache[i] = newn
		n.core_block.Addr[i] = newn.ThisAddr()
		return newn
	}
	if n.child_cache[i] != nil {
		return n.child_cache[i]
	}
	child, err := n.tr.LoadNode(n.core_block.Addr[i],
		n.core_block.CGeneration[i], n.ChildPW(), n.ChildStartTime(i))
	if err != nil {
		log.Panicf("%v", err)
	}
	child.parent = n
	n.child_cache[i] = child
	return child
}

//This function assumes that n is already new
func (n *QTreeNode) SetChild(idx uint16, c *QTreeNode) {
	if n.tr.gen == nil {
		log.Panicf("umm")
	}
	if n.isLeaf {
		log.Panicf("umm")
	}
	if !n.isNew {
		log.Panicf("uhuh lol?")
	}

	n.child_cache[idx] = c
	n.core_block.CGeneration[idx] = n.tr.Generation()
	if c == nil {
		n.core_block.Addr[idx] = 0
		n.core_block.Min[idx] = 0
		n.core_block.Max[idx] = 0
		n.core_block.Count[idx] = 0
		n.core_block.Mean[idx] = 0
	} else {
		c.parent = n
		if c.isLeaf {
			n.core_block.Addr[idx] = c.vector_block.Identifier
		} else {
			n.core_block.Addr[idx] = c.core_block.Identifier
		}
		//Note that a bunch of updates of the metrics inside the block need to
		//go here
		n.core_block.Min[idx] = c.OpMin()
		n.core_block.Max[idx] = c.OpMax()
		n.core_block.Count[idx], n.core_block.Mean[idx] = c.OpCountMean()
	}
}

//Here is where we would replace with fancy delta compression
// 合并两个有序序列
func (n *QTreeNode) MergeIntoVector(r []Record) {
	if !n.isNew {
		log.Panicf("bro... cmon")
	}
	//There is a special case: this can be called to insert into an empty leaf
	//don't bother being smart then
	if n.vector_block.Len == 0 {
		for i := 0; i < len(r); i++ {
			n.vector_block.Time[i] = r[i].Time
			n.vector_block.Value[i] = r[i].Val
		}
		n.vector_block.Len = uint16(len(r))
		return
	}
	curtimes := n.vector_block.Time
	curvals := n.vector_block.Value
	iDst := 0
	iVec := 0
	iRec := 0
	if len(r) == 0 {
		panic("zero record insert")
	}
	if n.vector_block.Len == 0 {
		panic("zero sized leaf")
	}
	for {
		if iRec == len(r) {
			//Dump vector
			for iVec < int(n.vector_block.Len) {
				n.vector_block.Time[iDst] = curtimes[iVec]
				n.vector_block.Value[iDst] = curvals[iVec]
				iDst++
				iVec++
			}
			break
		}
		if iVec == int(n.vector_block.Len) {
			//Dump records
			for iRec < len(r) {
				n.vector_block.Time[iDst] = r[iRec].Time
				n.vector_block.Value[iDst] = r[iRec].Val
				iDst++
				iRec++
			}
			break
		}
		if r[iRec].Time < curtimes[iVec] {
			n.vector_block.Time[iDst] = r[iRec].Time
			n.vector_block.Value[iDst] = r[iRec].Val
			iRec++
			iDst++
		} else {
			n.vector_block.Time[iDst] = curtimes[iVec]
			n.vector_block.Value[iDst] = curvals[iVec]
			iVec++
			iDst++
		}
	}
	n.vector_block.Len += uint16(len(r))
}

// 新生成一个原节点的拷贝节点
func (n *QTreeNode) AssertNewUpPatch() (*QTreeNode, error) {
	if n.isNew {
		//We assume that all parents are already ok
		return n, nil
	}

	//Ok we need to clone
	newn, err := n.clone()
	if err != nil {
		log.Panicf("%v", err)
	}

	//Does our parent need to also uppatch?
	if n.Parent() == nil {
		//We don't have a parent. We better be root
		if n.PointWidth() != n.tr.GetRootPW() {
			log.Panicf("WTF")
		}
	} else {
		npar, err := n.Parent().AssertNewUpPatch()
		if err != nil {
			log.Panicf("sigh")
		}
		//The parent might have changed. Update it
		newn.parent = npar
		//Get the IDX from the old parent
		//TODO(mpa) this operation is actually really slow. Change how we do it
		idx, err := n.FindParentIndex()
		if err != nil {
			log.Panicf("Could not find parent idx")
		}
		//Downlink
		newn.Parent().SetChild(idx, newn)
	}
	return newn, nil
}

//We need to create a core node, insert all the vector data into it,
//and patch up the parent
func (n *QTreeNode) ConvertToCore(newvals []Record) *QTreeNode {
	//log.Critical("CTC call")
	newn, err := n.tr.NewCoreNode(n.StartTime(), n.PointWidth())
	if err != nil {
		log.Panicf("%v", err)
	}
	n.parent.AssertNewUpPatch()
	newn.parent = n.parent
	idx, err := n.FindParentIndex()
	newn.Parent().SetChild(idx, newn)
	valset := make([]Record, int(n.vector_block.Len)+len(newvals))
	for i := 0; i < int(n.vector_block.Len); i++ {
		valset[i] = Record{n.vector_block.Time[i],
			n.vector_block.Value[i]}

	}
	base := n.vector_block.Len
	for i := 0; i < len(newvals); i++ {
		valset[base] = newvals[i]
		base++
	}
	sort.Sort(RecordSlice(valset))
	newn.InsertValues(valset)

	return newn
}

/**
 * This function is for inserting a large chunk of data. It is required
 * that the data is sorted, so we do that here
 */
func (tr *QTree) InsertValues(buffer Buffer, span time.Duration) (e error) {
	if tr.gen == nil {
		return ErrBadInsert
	}
	proc_records := buffer.ToSlice()
	idx := 0
	for _, v := range proc_records { // 把一些数据不正确、时间超出范围的数据点筛选出去
		if math.IsInf(v.Val, 0) || math.IsNaN(v.Val) {
			log.Debug("WARNING Got Inf/NaN insert value, dropping")
		} else if v.Time <= MinimumTime || v.Time >= MaximumTime {
			log.Debug("WARNING Got time out of range, dropping")
		} else {
			proc_records[idx] = v
			idx++
		}
	}
	proc_records = proc_records[:idx]
	if len(proc_records) == 0 {
		return ErrBadInsert
	}

	if !tr.initialized {
		tr.initialized = true
		// 选择一个合适的 K V
		K, V := tr.policy.DecideKAndV(buffer, span)
		tr.gen.New_SB.InitNewTS(K, V)
		rt, err := tr.NewCoreNode(ROOTSTART, tr.GetRootPW())
		if err != nil {
			log.Panicf("%v", err)
			return err
		}
		tr.root = rt
	}

	//
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered insertvalues panic", r)
			e = ErrBadInsert
		}
	}()
	sort.Sort(RecordSlice(proc_records))
	n, err := tr.root.InsertValues(proc_records) // 将数据插入节点中，但是还没有持久化到磁盘中
	if err != nil {
		log.Panicf("Root insert values: %v", err)
	}

	tr.root = n // 实际上每一次插入数据，都会在原本的数据上面加上一层，生成一个新的 root 节点
	tr.gen.UpdateRootAddr(n.ThisAddr())
	return nil
}

func (tr *QTree) DeleteRange(start int64, end int64) error {
	if tr.gen == nil {
		return ErrBadDelete
	}
	if !tr.initialized { // 没数据凑什么热闹啊
		return nil
	}
	n := tr.root.DeleteRange(start, end)
	tr.root = n
	if n == nil {
		tr.gen.UpdateRootAddr(0)
	} else {
		tr.gen.UpdateRootAddr(n.ThisAddr())
	}
	return nil
}

/**
 * the process is:
 * call insertvalues - returns new QTreeNode.
 *   this must have: address, stats
 *   and it must have put whatever it touched in the generation
 * replace it in the child cache, change address + stats
 *   and return to parent
 */
func (n *QTreeNode) InsertValues(records []Record) (*QTreeNode, error) {
	// 注意，records 是已经排好序的
	//log.Debug("InsertValues called on pw(%v) with %v records @%08x",
	//	n.PointWidth(), len(records), n.ThisAddr())
	//log.Debug("IV ADDR: %s", n.TreePath())
	////First determine if any of the records are outside our window
	//This is debugging, it won't even work if the records aren't sorted
	// 打印一些无关紧要的日志信息

	if !n.isLeaf {
		if records[0].Time < n.StartTime() {
			//Actually I don't think we can be less than the start.
			log.Panicf("Bad window <")
		}
		if records[len(records)-1].Time >= n.StartTime()+((1<<n.PointWidth())*int64(n.tr.GetKFactor())) {
			log.Debug("FE.")
			log.Debug("Node window s=%v e=%v", n.StartTime(),
				n.StartTime()+((1<<n.PointWidth())*int64(n.tr.GetKFactor())))
			log.Debug("record time: %v", records[len(records)-1].Time)
			log.Panicf("Bad window >=")
		}
	}
	// 核心插入逻辑
	if n.isLeaf {
		//log.Debug("insertin values in leaf")
		if int(n.vector_block.Len)+len(records) > n.tr.GetVSize() && n.PointWidth() != 0 {
			//log.Debug("need to convert leaf to a core");
			//log.Debug("because %v + %v",n.vector_block.Len, len(records))
			//log.Debug("Converting pw %v to core", n.PointWidth())
			n = n.ConvertToCore(records) // 假如点太多那么引入额外的中间节点，再将数据插入到中间节点下属的叶节点中
			return n, nil
		} else {
			if n.PointWidth() == 0 && int(n.vector_block.Len)+len(records) > n.tr.GetVSize() {
				truncidx := n.tr.GetVSize() - int(n.vector_block.Len)
				if truncidx == 0 {
					log.Critical("Truncating - full!!")
					return n, nil
				}
				log.Critical("Truncating insert due to PW0 overflow!!")
				records = records[:truncidx]
			}
			//log.Debug("inserting %d records into pw(%v) vector", len(records),n.PointWidth())
			newn, err := n.AssertNewUpPatch()
			if err != nil {
				log.Panicf("Uppatch failed: ", err)
			}
			n = newn
			n.MergeIntoVector(records)
			return n, nil
		}
	} else {
		//log.Debug("inserting valus in core")
		//We are a core node
		newn, err := n.AssertNewUpPatch() // 生成一个新的当前节点
		if err != nil {
			log.Panicf("ANUP: %v", err)
		}
		n := newn
		lidx := 0
		lbuckt := n.ClampBucket(records[0].Time)
		for idx := 1; idx < len(records); idx++ {
			r := records[idx]
			//log.Debug("iter: %v, %v", idx, r)
			buckt := n.ClampBucket(r.Time)
			if buckt != lbuckt {
				//log.Debug("records spanning bucket. flushing to child %v", lbuckt)
				//Next bucket has started
				childisleaf := idx-lidx < n.tr.GetVSize()
				if n.ChildPW() == 0 {
					childisleaf = true
				}
				newchild, err := n.wchild(lbuckt, childisleaf).InsertValues(records[lidx:idx])
				if err != nil {
					log.Panicf("%v", err)
				}
				n.SetChild(lbuckt, newchild) //This should set parent link too
				lidx = idx
				lbuckt = buckt
			}
		}
		//log.Debug("reched end of records. flushing to child %v", buckt)
		newchild, err := n.wchild(lbuckt, (len(records)-lidx) < n.tr.GetVSize()).InsertValues(records[lidx:])
		//log.Debug("Address of new child was %08x", newchild.ThisAddr())
		if err != nil {
			log.Panicf("%v", err)
		}
		n.SetChild(lbuckt, newchild)

		return n, nil
	}
}

var ErrBadTimeRange = errors.New("Invalid time range")

//start is inclusive, end is exclusive. To query a specific nanosecond, query (n, n+1)
func (tr *QTree) ReadStandardValuesCI(rv chan Record, err chan error, start int64, end int64) {
	if tr.root != nil {
		tr.root.ReadStandardValuesCI(rv, err, start, end)
	}
	close(rv)
	close(err)
}

// 读取某个时间区间的数据
func (tr *QTree) ReadStandardValuesBlock(start int64, end int64) ([]Record, error) {
	rv := make([]Record, 0, 256)
	recordc := make(chan Record)
	errc := make(chan error)
	var err error
	busy := true
	go tr.ReadStandardValuesCI(recordc, errc, start, end) // 异步加载数据，加载到的数据传入的 record chan 中，在下面的轮询中读取 chan 中的数据
	for busy {
		select {
		case e, _ := <-errc:
			if e != nil {
				err = e
				busy = false
			}
		case r, r_ok := <-recordc:
			if !r_ok {
				busy = false
			} else {
				rv = append(rv, r)
			}
		}
	}
	return rv, err
}

type StatRecord struct {
	Time  int64 //This is at the start of the record
	Count uint64
	Min   float64
	Mean  float64
	Max   float64
}

type WindowContext struct {
	Time   int64
	Count  uint64
	Min    float64
	Total  float64
	Max    float64
	Active bool
	Done   bool
}

func (tr *QTree) QueryStatisticalValues(rv chan StatRecord, err chan error,
	start int64, end int64, pw uint8) {
	//Remember end is inclusive for QSV
	if tr.root != nil {
		tr.root.QueryStatisticalValues(rv, err, start, end, pw)
	}
	close(err)
}

func (tr *QTree) QueryStatisticalValuesBlock(start int64, end int64, pw uint8) ([]StatRecord, error) {
	rv := make([]StatRecord, 0, 256)
	recordc := make(chan StatRecord, 500)
	errc := make(chan error)
	var err error
	busy := true
	go tr.QueryStatisticalValues(recordc, errc, start, end, pw)
	for busy {
		select {
		case e, _ := <-errc:
			if e != nil {
				err = e
				busy = false
			}
		case r, r_ok := <-recordc:
			if !r_ok {
				busy = false
			} else {
				rv = append(rv, r)
			}
		}
	}
	return rv, err
}

type childpromise struct {
	RC  chan StatRecord
	Hnd *QTreeNode
	Idx uint16
}

// 原理和 ReadStandardValuesCI 几乎是一致的
func (n *QTreeNode) QueryStatisticalValues(rv chan StatRecord, err chan error,
	start int64, end int64, pw uint8) {
	if n.isLeaf {
		for idx := 0; idx < int(n.vector_block.Len); idx++ {
			if n.vector_block.Time[idx] < start {
				continue
			}
			if n.vector_block.Time[idx] >= end {
				break
			}
			// 对于每一个符合时间范围的点
			b := n.ClampVBucket(n.vector_block.Time[idx], pw)
			count, min, mean, max := n.OpReduce(pw, uint64(b))
			if count != 0 {
				rv <- StatRecord{Time: n.ArbitraryStartTime(b, pw),
					Count: count,
					Min:   min,
					Mean:  mean,
					Max:   max,
				}
				//Skip over records in the vector that the PW included
				idx += int(count - 1)
			}
		}
		close(rv)
		/*
			sb := n.ClampVBucket(start, pw)
			eb := n.ClampVBucket(end, pw)
			for b := sb; b <= eb; b++ {
				count, min, mean, max := n.OpReduce(pw, uint64(b))
				if count != 0 {
					rv <- StatRecord{Time: n.ArbitraryStartTime(b, pw),
						Count: count,
						Min:   min,
						Mean:  mean,
						Max:   max,
					}
				}
			}*/
	} else {
		//Ok we are at the correct level and we are a core
		sb := n.ClampBucket(start) //TODO check this function handles out of range
		eb := n.ClampBucket(end)
		recurse := pw < n.PointWidth()
		if recurse {
			//Parallel resolution of children
			//don't use n.Child() because its not threadsafe
			/*for b := sb; b <= eb; b++ {
				go n.PrebufferChild(b)
			}*/

			var childslices []childpromise
			for b := sb; b <= eb; b++ {
				c := n.Child(b)
				if c != nil {
					childrv := make(chan StatRecord, 500)
					go c.QueryStatisticalValues(childrv, err, start, end, pw)
					childslices = append(childslices, childpromise{childrv, c, b})
				}
			}
			for _, prom := range childslices {
				for v := range prom.RC {
					rv <- v
				}
				prom.Hnd.Free()
				n.child_cache[prom.Idx] = nil
			}
			close(rv)
		} else {
			pwdelta := pw - n.PointWidth()
			sidx := sb >> pwdelta
			eidx := eb >> pwdelta
			for b := sidx; b <= eidx; b++ {
				count, min, mean, max := n.OpReduce(pw, uint64(b))
				if count != 0 {
					rv <- StatRecord{Time: n.ChildStartTime(b << pwdelta),
						Count: count,
						Min:   min,
						Mean:  mean,
						Max:   max,
					}
				}
			}
			close(rv)
		}
	}
}

//Although we keep caches of datablocks in the bstore, we can't actually free them until
//they are unreferenced. This dropcache actually just makes sure they are unreferenced
func (n *QTreeNode) Free() {
	if n.isLeaf {
		n.tr.bs.FreeVectorblock(&n.vector_block)
	} else {
		for i, c := range n.child_cache {
			if c != nil {
				c.Free()
				n.child_cache[i] = nil
			}
		}
		n.tr.bs.FreeCoreblock(&n.core_block)
	}

}

// 读取某个子树在时间区间中的所有数据
func (n *QTreeNode) ReadStandardValuesCI(rv chan Record, err chan error,
	start int64, end int64) {
	if end <= start {
		err <- ErrBadTimeRange
		return
	}
	if n.isLeaf {
		//log.Debug("rsvci = leaf len(%v)", n.vector_block.Len)
		//Currently going under assumption that buckets are sorted
		//TODO replace with binary searches
		for i := 0; i < int(n.vector_block.Len); i++ {
			if n.vector_block.Time[i] >= start {
				if n.vector_block.Time[i] < end {
					rv <- Record{n.vector_block.Time[i], n.vector_block.Value[i]}
				} else {
					//Hitting a value past end means we are done with the query as a whole
					//we just need to clean up our memory now
					return
				}
			}
		}
	} else {
		//log.Debug("rsvci = core")

		//We are a core
		sbuck := uint16(0)
		if start > n.StartTime() {
			if start >= n.EndTime() {
				log.Panicf("hmmm")
			}
			sbuck = n.ClampBucket(start)
		}
		ebuck := uint16(n.tr.GetKFactor())
		if end < n.EndTime() {
			if end < n.StartTime() {
				log.Panicf("hmm")
			}
			ebuck = n.ClampBucket(end) + 1
		}
		//log.Debug("rsvci s/e %v/%v",sbuck, ebuck)
		for buck := sbuck; buck < ebuck; buck++ {
			//log.Debug("walking over child %v", buck)
			c := n.Child(buck)
			if c != nil {
				//log.Debug("child existed")
				//log.Debug("rscvi descending from pw(%v) into [%v]", n.PointWidth(),buck)
				c.ReadStandardValuesCI(rv, err, start, end)
				c.Free()
				n.child_cache[buck] = nil
			} else {
				//log.Debug("child was nil")
			}
		}
	}
}

func (n *QTreeNode) updateWindowContextWholeChild(child uint16, ctx *WindowContext) {

	if (n.core_block.Max[child] > ctx.Max || ctx.Count == 0) && n.core_block.Count[child] != 0 {
		ctx.Max = n.core_block.Max[child]
	}
	if (n.core_block.Min[child] < ctx.Min || ctx.Count == 0) && n.core_block.Count[child] != 0 {
		ctx.Min = n.core_block.Min[child]
	}
	ctx.Total += n.core_block.Mean[child] * float64(n.core_block.Count[child])
	ctx.Count += n.core_block.Count[child]
}
func (n *QTreeNode) emitWindowContext(rv chan StatRecord, width uint64, ctx *WindowContext) {
	var mean float64
	if ctx.Count != 0 {
		mean = ctx.Total / float64(ctx.Count)
	}
	res := StatRecord{
		Count: ctx.Count,
		Min:   ctx.Min,
		Max:   ctx.Max,
		Mean:  mean,
		Time:  ctx.Time,
	}
	rv <- res
	ctx.Active = true
	ctx.Min = 0
	ctx.Total = 0
	ctx.Max = 0
	ctx.Count = 0
	ctx.Time += int64(width)
}

//QueryWindow queries this node for an arbitrary window of time. ctx must be initialized, especially with Time.
//Holes will be emitted as blank records
func (n *QTreeNode) QueryWindow(end int64, nxtstart *int64, width uint64, depth uint8, rv chan StatRecord, ctx *WindowContext) {
	if !n.isLeaf {
		//We are core
		var buckid uint16
		for buckid = 0; buckid < uint16(n.tr.GetKFactor()); buckid++ {
			//EndTime is actually start of next
			if n.ChildEndTime(buckid) <= *nxtstart {
				//This bucket is wholly contained in the 'current' window.
				//If we are not active, that does not matter
				if !ctx.Active {
					continue
				}
				//Update the current context
				n.updateWindowContextWholeChild(buckid, ctx)
			}
			if n.ChildEndTime(buckid) == *nxtstart {
				//We will have updated the context above, but we also now
				//need to emit, because there is nothing left in the context
				//We can cleanly emit and start new window without going into child
				//because the childEndTime exactly equals the next start
				//or because the child in question does not exist
				n.emitWindowContext(rv, width, ctx)
				//Check it wasn't the last
				if *nxtstart >= end {
					ctx.Done = true
					return
				}
				*nxtstart += int64(width)
				//At this point we have a new context, we can continue to next loop
				//iteration
				continue
			} else if n.ChildEndTime(buckid) > *nxtstart {
				//The bucket went past nxtstart, so we need to fragment
				if true /*XTAG replace with depth check*/ {
					//Now, we might want
					//They could be equal if next window started exactly after
					//this child. That would mean we don't recurse
					//As it turns out, we must recurse before emitting
					if n.HasChild(buckid) {
						n.Child(buckid).QueryWindow(end, nxtstart, width, depth, rv, ctx)
						if ctx.Done {
							return
						}
					} else {
						//We would have had a child that did the emit + restart for us
						//Possibly several times over, but there is a hole, so we need
						//to emulate all of that.
						if !ctx.Active {
							//Our current time is ctx.Time
							//We know that ChildEndTime is greater than nxttime
							//We can just set time to nxttime, and then continue with
							//the active loop
							ctx.Time = *nxtstart
							ctx.Active = true
							*nxtstart += int64(width)
						}
						//We are definitely active now
						//For every nxtstart less than this (missing) bucket's end time,
						//emit a window
						for *nxtstart <= n.ChildEndTime(buckid) {
							n.emitWindowContext(rv, width, ctx)
							if ctx.Time != *nxtstart {
								panic("LOLWUT")
							}
							*nxtstart += int64(width)
							if *nxtstart >= end {
								ctx.Done = true
								return
							}
						}
					}
				} //end depth check
			} //End bucket > nxtstart
		} //For loop over children
	} else {
		//We are leaf
		//There could be multiple windows within us
		var i uint16
		for i = 0; i < n.vector_block.Len; i++ {

			//We use this twice, pull it out
			add := func() {
				ctx.Total += n.vector_block.Value[i]
				if n.vector_block.Value[i] < ctx.Min || ctx.Count == 0 {
					ctx.Min = n.vector_block.Value[i]
				}
				if n.vector_block.Value[i] > ctx.Max || ctx.Count == 0 {
					ctx.Max = n.vector_block.Value[i]
				}
				ctx.Count++
			}

			//Check if part of active ctx
			if n.vector_block.Time[i] < *nxtstart {
				//This is part of the previous window
				if ctx.Active {
					add()
				} else {
					//This is before our active time
					continue
				}
			} else {
				//We have crossed a window boundary
				if ctx.Active {
					//We need to emit the window
					n.emitWindowContext(rv, width, ctx)
					//Check it wasn't the last
					if *nxtstart >= end {
						ctx.Done = true
						return
					}
					*nxtstart += int64(width)
				} else {
					//This is the first window
					ctx.Active = true
					*nxtstart += int64(width)
				}
				//If we are here, this point needs to be added to the context
				add()
			}
		}
	}
}

//QueryWindow queries for windows between start and end, with an explicit (arbitrary) width. End is exclusive
func (tr *QTree) QueryWindow(start int64, end int64, width uint64, depth uint8, rv chan StatRecord) {
	ctx := &WindowContext{Time: start}
	var nxtstart = start
	if tr.root != nil {
		tr.root.QueryWindow(end, &nxtstart, width, depth, rv, ctx)
	}
	close(rv)
}
