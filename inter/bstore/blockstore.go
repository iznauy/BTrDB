package bstore

import (
	"errors"
	"github.com/iznauy/BTrDB/brain"
	"github.com/iznauy/BTrDB/brain/types"
	"github.com/iznauy/BTrDB/inter/metaprovider"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/iznauy/BTrDB/inter/bprovider"
	"github.com/iznauy/BTrDB/inter/fileprovider"
	"github.com/pborman/uuid"
)

const LatestGeneration = uint64(^(uint64(0)))

// 将 uuid 转化成比特数组
func UUIDToMapKey(id uuid.UUID) [16]byte {
	rv := [16]byte{}
	copy(rv[:], id)
	return rv
}

type BlockStore struct {
	_wlocks map[[16]byte]*sync.Mutex
	glock   sync.RWMutex

	params map[string]string

	cachemap    map[uint64]*CacheItem
	cacheold    *CacheItem
	cachenew    *CacheItem
	cachemtx    sync.Mutex
	cachelen    uint64
	cachemax    uint64
	cachemaxmtx sync.RWMutex

	cachemiss uint64
	cachehit  uint64

	meta  metaprovider.MetaProvider
	store bprovider.StorageProvider
	alloc chan uint64
}

var block_buf_pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

var ErrDatablockNotFound = errors.New("Coreblock not found")
var ErrGenerationNotFound = errors.New("Generation not found")

/* A generation stores all the information acquired during a write pass.
 * A superblock contains all the information required to navigate a tree.
 */
type Generation struct {
	// 某一次更新操作中的状态信息，包括分配的所有的数据块、新生成的超级块等等，最后会在 Link 阶段刷新到底层存储当中
	Cur_SB     *Superblock
	New_SB     *Superblock
	cblocks    []*Coreblock
	vblocks    []*Vectorblock
	blockstore *BlockStore
	flushed    bool
	newts      bool
}

func (g *Generation) UpdateRootAddr(addr uint64) {
	//log.Printf("updateaddr called (%v)",addr)
	g.New_SB.root = addr
}
func (g *Generation) Uuid() *uuid.UUID {
	return &g.New_SB.uuid
}

func (g *Generation) Number() uint64 { // 版本号信息
	return g.New_SB.gen
}

func (g *Generation) IsNewTS() bool {
	return g.newts
}

func NewBlockStore(params map[string]string) (*BlockStore, error) {
	bs := BlockStore{}
	bs.params = params

	if params["metaprovider"] == "mysql" {
		meta, err := metaprovider.NewMySQLMetaProvider(params)
		if err != nil {
			lg.Panicf("init mysql metaprovider error: %v", err)
		}
		bs.meta = meta
	} else if params["metaprovider"] == "mongodb" {
		meta, err := metaprovider.NewMongoDBMetaProvider(params)
		if err != nil {
			lg.Panicf("init mongodb metaprovider error: %v", err)
		}
		bs.meta = meta
	} else if params["metaprovider"] == "mem" {
		meta, err := metaprovider.NewMemMetaProvider(params)
		if err != nil {
			lg.Panicf("init mem metaprovider error: %v", err)
		}
		bs.meta = meta
	}

	bs._wlocks = make(map[[16]byte]*sync.Mutex)

	// 虚拟地址分配器
	bs.alloc = make(chan uint64, 256)
	go func() {
		relocation_addr := uint64(RELOCATION_BASE) // 虚拟地址的地址空间是不在物理地址的地址空间之内的
		for {
			bs.alloc <- relocation_addr
			relocation_addr += 1
			if relocation_addr < RELOCATION_BASE { // 指的是 relocation_addr 越界归零了
				relocation_addr = RELOCATION_BASE
			}
		}
	}()

	// 初始化底层数据存储
	bs.store = new(fileprovider.FileStorageProvider)
	bs.store.Initialize(params)

	// 初始化 cache
	cachesz, err := strconv.ParseInt(params["cachesize"], 0, 64)
	if err != nil {
		lg.Panicf("Bad cache size: %v", err)
	}
	bs.initCache(uint64(cachesz))

	// 上报统计信息
	go bs.emitEvent()
	// 动态变化 cache
	go bs.vary()

	return &bs, nil
}

func (bs *BlockStore) emitEvent() {
	for {
		time.Sleep(1 * time.Second)

		e := &types.Event{
			Type:   types.CacheNotice,
			Source: nil,
			Time:   time.Now(),
			Params: map[string]interface{}{
				"cache_hit":      bs.cachehit,
				"cache_miss":     bs.cachemiss,
				"cache_size":     bs.cachemax,
				"leaf_count":     uint64(0),
				"non_leaf_count": bs.cachelen,
			},
		}
		brain.B.Emit(e)
	}
}

func (bs *BlockStore) vary() {
	for {
		// 定期激活
		time.Sleep(10 * time.Second)
		// 获取最新决策
		cacheMaxSize := brain.B.GetCacheMaxSize()
		bs.cachemaxmtx.Lock()
		bs.cachemax = cacheMaxSize
		bs.cachemaxmtx.Unlock()
	}
}

func (bs *BlockStore) GetCacheMaxSize() uint64 {
	bs.cachemaxmtx.RLock()
	cachemax := bs.cachemax
	bs.cachemaxmtx.RUnlock()
	return cachemax
}

/*
 * This obtains a generation, blocking if necessary
 */
func (bs *BlockStore) ObtainGeneration(id uuid.UUID) *Generation {
	//The first thing we do is obtain a write lock on the UUID, as a generation
	//represents a lock
	mk := UUIDToMapKey(id)
	bs.glock.RLock()
	mtx, ok := bs._wlocks[mk]
	bs.glock.RUnlock()
	if !ok {
		//Mutex doesn't exist so is unlocked
		mtx := new(sync.Mutex)
		mtx.Lock()
		bs.glock.Lock()
		bs._wlocks[mk] = mtx
		bs.glock.Unlock()
	} else {
		mtx.Lock()
	}

	gen := &Generation{
		cblocks: make([]*Coreblock, 0, 10),
		vblocks: make([]*Vectorblock, 0, 10),
	}

	meta, err := bs.meta.GetLatestMeta(id.String())
	if err == metaprovider.MetaNotFound {
		gen.Cur_SB = NewSuperblock(id)
		gen.newts = true
	} else if err != nil {
		lg.Panicf("meta provider error: %v", err)
	} else {
		sb := Superblock{
			uuid: id,
			root: meta.Root,
			k:    meta.K,
			v:    meta.V,
			gen:  meta.Version,
		}
		gen.Cur_SB = &sb
		gen.newts = false
	}

	gen.New_SB = gen.Cur_SB.Clone()
	gen.New_SB.gen = gen.Cur_SB.gen + 1 // 版本号喜加一
	gen.blockstore = bs
	return gen
}

//The returned address map is primarily for unit testing
func (gen *Generation) Commit() (map[uint64]uint64, error) {
	if gen.flushed {
		return nil, errors.New("Already Flushed")
	}

	//then := time.Now()
	// 写入底层存储
	address_map := LinkAndStore([]byte(*gen.Uuid()), gen.blockstore, gen.blockstore.store, gen.vblocks, gen.cblocks)
	// 写元信息到 mongodb
	rootaddr, ok := address_map[gen.New_SB.root]
	if !ok {
		lg.Panic("Could not obtain root address")
	}
	gen.New_SB.root = rootaddr

	gen.vblocks = nil
	gen.cblocks = nil

	meta := &metaprovider.Meta{
		Uuid:    gen.New_SB.uuid.String(),
		Version: gen.New_SB.gen,
		Root:    gen.New_SB.root,
		K:       gen.New_SB.K(),
		V:       gen.New_SB.V(),
	}
	if err := gen.blockstore.meta.InsertMeta(meta); err != nil {
		lg.Panic(err)
	}

	gen.flushed = true

	gen.blockstore.glock.RLock()
	gen.blockstore._wlocks[UUIDToMapKey(*gen.Uuid())].Unlock()
	gen.blockstore.glock.RUnlock()
	return address_map, nil
}

func (bs *BlockStore) allocateBlock() uint64 {
	relocation_address := <-bs.alloc
	return relocation_address
}

/**
 * The real function is supposed to allocate an address for the data
 * block, reserving it on disk, and then give back the data block that
 * can be filled in
 * This stub makes up an address, and mongo pretends its real
 */
func (gen *Generation) AllocateCoreblock() (*Coreblock, error) {
	cblock := &Coreblock{}
	cblock.Addr = make([]uint64, gen.New_SB.k)
	cblock.Count = make([]uint64, gen.New_SB.k)
	cblock.Min = make([]float64, gen.New_SB.k)
	cblock.Mean = make([]float64, gen.New_SB.k)
	cblock.Max = make([]float64, gen.New_SB.k)
	cblock.CGeneration = make([]uint64, gen.New_SB.k)
	cblock.Identifier = gen.blockstore.allocateBlock()
	cblock.Generation = gen.Number()
	gen.cblocks = append(gen.cblocks, cblock)
	return cblock, nil
}

func (gen *Generation) AllocateVectorblock() (*Vectorblock, error) {
	vblock := &Vectorblock{}
	vblock.Time = make([]int64, gen.New_SB.v)
	vblock.Value = make([]float64, gen.New_SB.v)
	vblock.Identifier = gen.blockstore.allocateBlock()
	vblock.Generation = gen.Number()
	gen.vblocks = append(gen.vblocks, vblock)
	return vblock, nil
}

func (bs *BlockStore) FreeCoreblock(cb **Coreblock) {
	*cb = nil
}

func (bs *BlockStore) FreeVectorblock(vb **Vectorblock) {
	*vb = nil
}

func (bs *BlockStore) ReadDatablock(uuid uuid.UUID, addr uint64, impl_Generation uint64, impl_Pointwidth uint8, impl_StartTime int64, k int, v int) Datablock {
	//Try hit the cache first
	db := bs.cacheGet(addr)
	if db != nil {
		return db
	}

	// emit read block event
	e := &types.Event{
		Type:   types.ReadBlock,
		Source: uuid,
		Time:   time.Now(),
		Params: map[string]interface{}{},
	}
	defer brain.B.Emit(e)

	syncbuf := block_buf_pool.Get().([]byte)
	trimbuf := bs.store.Read([]byte(uuid), addr, syncbuf)
	switch DatablockGetBufferType(trimbuf) {
	case Core:
		rv := &Coreblock{}
		rv.Addr = make([]uint64, k)
		rv.Count = make([]uint64, k)
		rv.Min = make([]float64, k)
		rv.Mean = make([]float64, k)
		rv.Max = make([]float64, k)
		rv.CGeneration = make([]uint64, k)
		rv.Deserialize(trimbuf)
		block_buf_pool.Put(syncbuf)
		rv.Identifier = addr
		rv.Generation = impl_Generation
		rv.PointWidth = impl_Pointwidth
		rv.StartTime = impl_StartTime
		bs.cachePut(addr, rv)
		e.Params["core_count"] = uint64(1)
		return rv
	case Vector:
		rv := &Vectorblock{}
		rv.Time = make([]int64, v)
		rv.Value = make([]float64, v)
		rv.Deserialize(trimbuf)
		block_buf_pool.Put(syncbuf)
		rv.Identifier = addr
		rv.Generation = impl_Generation
		rv.PointWidth = impl_Pointwidth
		rv.StartTime = impl_StartTime
		bs.cachePut(addr, rv)
		e.Params["vector_count"] = uint64(1)
		return rv
	}
	lg.Panic("Strange datablock type")
	return nil
}

// 从存储引擎中加载超级块信息
func (bs *BlockStore) LoadSuperblock(id uuid.UUID, generation uint64) *Superblock {
	var (
		meta *metaprovider.Meta
		err  error
	)
	if generation == LatestGeneration {
		if meta, err = bs.meta.GetLatestMeta(id.String()); err != nil {
			if err == metaprovider.MetaNotFound {
				lg.Info("superblock not found!")
				return nil
			} else {
				lg.Panic(err)
			}
		}
	} else {
		if meta, err = bs.meta.GetMeta(id.String(), generation); err != nil {
			if err == metaprovider.MetaNotFound {
				return nil
			} else {
				lg.Panic(err)
			}
		}
	}
	rv := Superblock{
		uuid:     id,
		gen:      meta.Version,
		root:     meta.Root,
		k:        meta.K,
		v:        meta.V,
		unlinked: meta.Unlinked,
	}
	return &rv
}

func CreateDatabase(params map[string]string) {
	if params["metaprovider"] == "mysql" {
		metaprovider.CreateMySQLMetaDatabase(params)
	} else if params["metaprovider"] == "mongodb" {
		metaprovider.CreateMongoDBMetaDatabase(params)
	}

	if err := os.MkdirAll(params["dbpath"], 0755); err != nil {
		lg.Panic(err)
	}
	fp := new(fileprovider.FileStorageProvider)
	err := fp.CreateDatabase(params)
	if err != nil {
		lg.Criticalf("Error on create: %v", err)
		os.Exit(1)
	}
}
