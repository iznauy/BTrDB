package bstore

import (
	"fmt"
	"time"
)

type CacheItem struct {
	val   Datablock  // 数据块
	vaddr uint64     // 虚拟地址
	newer *CacheItem // cache 形成了一个双向链表，便于实现 LRU 淘汰算法
	older *CacheItem
}

func (bs *BlockStore) initCache(size uint64) {
	bs.cachemax = size
	bs.cachemap = make(map[uint64]*CacheItem, size) // index: virtual address (虚拟地址)
	go func() {
		for {
			lg.Info(fmt.Sprintf("Cachestats: %d misses, %d hits, %.2f %%",
				bs.cachemiss, bs.cachehit, (float64(bs.cachehit*100) / float64(bs.cachemiss+bs.cachehit))))
			time.Sleep(5 * time.Second)
		}
	}()
}

// 双向链表插入删除
func (bs *BlockStore) cachePromote(i *CacheItem) {
	// 如果该 cache 已经在最前面，就不处理
	if bs.cachenew == i {
		//Already at front
		return
	}
	// cache 不在最前，则将自己从双向链表中删除
	if i.newer != nil {
		i.newer.older = i.older
	}
	if i.older != nil {
		i.older.newer = i.newer
	}
	if bs.cacheold == i && i.newer != nil {
		//This was the tail of a list longer than 1
		bs.cacheold = i.newer
	} else if bs.cacheold == nil {
		//This was/is the only item in the list
		bs.cacheold = i
	}

	i.newer = nil
	// 将自己放到链表头
	i.older = bs.cachenew
	if bs.cachenew != nil {
		bs.cachenew.newer = i
	}
	bs.cachenew = i
}

func (bs *BlockStore) cachePut(vaddr uint64, item Datablock) {
	if bs.GetCacheMaxSize() == 0 {
		return
	}
	bs.cachemtx.Lock()
	i, ok := bs.cachemap[vaddr]
	if ok {
		bs.cachePromote(i)
	} else {
		i = &CacheItem{
			val:   item,
			vaddr: vaddr,
		}
		bs.cachemap[vaddr] = i
		bs.cachePromote(i)
		bs.cachelen++
		bs.cacheCheckCap()
	}
	bs.cachemtx.Unlock()
}

func (bs *BlockStore) cacheGet(vaddr uint64) Datablock {
	if bs.GetCacheMaxSize() == 0 {
		bs.cachemiss++
		return nil
	}
	bs.cachemtx.Lock()
	rv, ok := bs.cachemap[vaddr]
	if ok {
		bs.cachePromote(rv)
	}
	bs.cachemtx.Unlock()
	if ok {
		bs.cachehit++
		return rv.val
	} else {
		bs.cachemiss++
		return nil
	}
}

//debug function
// Ignore
func (bs *BlockStore) walkCache() {
	fw := 0
	bw := 0
	it := bs.cachenew
	for {
		if it == nil {
			break
		}
		fw++
		if it.older == nil {
			lg.Info("fw walked to end, compare %p/%p", it, bs.cacheold)
		}
		it = it.older
	}
	it = bs.cacheold
	for {
		if it == nil {
			break
		}
		bw++
		if it.newer == nil {
			lg.Info("bw walked to end, compare %p/%p", it, bs.cachenew)
		}
		it = it.newer
	}
	lg.Info("Walked cache fw=%v, bw=%v, map=%v", fw, bw, len(bs.cachemap))
}

//This must be called with the mutex held
// cache 块数量超过上限，则依据 LRU 原则淘汰最近不使用的 cache
func (bs *BlockStore) cacheCheckCap() {
	cacheMax := bs.GetCacheMaxSize()
	c := 0
	for bs.cachelen > cacheMax && c < 100 { // 最多淘汰 100 个
		i := bs.cacheold
		delete(bs.cachemap, i.vaddr)
		if i.newer != nil {
			i.newer.older = nil
		}
		bs.cacheold = i.newer
		bs.cachelen--
		c++
	}
}
