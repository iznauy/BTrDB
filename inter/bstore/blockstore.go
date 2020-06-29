package bstore

import (
	"errors"
	"os"
	"strconv"
	"sync"

	"github.com/UlricQin/btrdb/inter/bprovider"
	"github.com/UlricQin/btrdb/inter/fileprovider"
	"github.com/pborman/uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const LatestGeneration = uint64(^(uint64(0)))

// 将 uuid 转化成比特数组
func UUIDToMapKey(id uuid.UUID) [16]byte {
	rv := [16]byte{}
	copy(rv[:], id)
	return rv
}

type BlockStore struct {
	ses     *mgo.Session
	db      *mgo.Database
	_wlocks map[[16]byte]*sync.Mutex
	glock   sync.RWMutex

	params map[string]string

	cachemap map[uint64]*CacheItem
	cacheold *CacheItem
	cachenew *CacheItem
	cachemtx sync.Mutex
	cachelen uint64
	cachemax uint64

	cachemiss uint64
	cachehit  uint64

	store bprovider.StorageProvider
	alloc chan uint64
}

var block_buf_pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, DBSIZE+5)
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
}

func (g *Generation) UpdateRootAddr(addr uint64) {
	//log.Printf("updateaddr called (%v)",addr)
	g.New_SB.root = addr
}
func (g *Generation) Uuid() *uuid.UUID {
	return &g.Cur_SB.uuid
}

func (g *Generation) Number() uint64 { // 版本号信息
	return g.New_SB.gen
}

func (bs *BlockStore) UnlinkGenerations(id uuid.UUID, sgen uint64, egen uint64) error {
	iter := bs.db.C("superblocks").Find(bson.M{"uuid": id.String(), "gen": bson.M{"$gte": sgen, "$lt": egen}, "unlinked": false}).Iter()
	rs := fake_sblock{}
	for iter.Next(&rs) {
		rs.Unlinked = true
		_, err := bs.db.C("superblocks").Upsert(bson.M{"uuid": id.String(), "gen": rs.Gen}, rs)
		if err != nil {
			lg.Panic(err)
		}
	}
	return nil
}

func NewBlockStore(params map[string]string) (*BlockStore, error) {
	bs := BlockStore{}
	bs.params = params

	// 初始化 mongo db 存储元数据细腻
	ses, err := mgo.Dial(params["mongoserver"])
	if err != nil {
		return nil, err
	}
	bs.ses = ses
	bs.db = ses.DB(params["collection"])
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
		lg.Panic("Bad cache size: %v", err)
	}
	bs.initCache(uint64(cachesz))

	return &bs, nil
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
		cblocks: make([]*Coreblock, 0, 8192),
		vblocks: make([]*Vectorblock, 0, 8192),
	}
	//We need a generation. Lets see if one is on disk
	qry := bs.db.C("superblocks").Find(bson.M{"uuid": id.String()})
	rs := fake_sblock{}
	qerr := qry.Sort("-gen").One(&rs)
	if qerr == mgo.ErrNotFound {
		lg.Info("no superblock found for %v", id.String())
		//Ok just create a new superblock/generation
		gen.Cur_SB = NewSuperblock(id)
	} else if qerr != nil {
		//Well thats more serious
		lg.Panic("Mongodb error: %v", qerr)
	} else {
		//Ok we have a superblock, pop the gen
		//log.Info("Found a superblock for %v", id.String())
		sb := Superblock{
			uuid: id,
			root: rs.Root,
			gen:  rs.Gen,
		}
		gen.Cur_SB = &sb
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
	//dt := time.Now().Sub(then)

	//log.Info("(LAS %4dus %dc%dv) ins blk u=%v gen=%v root=0x%016x",
	//	uint64(dt/time.Microsecond), len(gen.cblocks), len(gen.vblocks), gen.Uuid().String(), gen.Number(), rootaddr)
	/*if len(gen.vblocks) > 100 {
		total := 0
		for _, v:= range gen.vblocks {
			total += int(v.Len)
		}
		log.Critical("Triggered vblock examination: %v blocks, %v points, %v avg", len(gen.vblocks), total, total/len(gen.vblocks))
	}*/
	gen.vblocks = nil
	gen.cblocks = nil
	fsb := fake_sblock{
		Uuid: gen.New_SB.uuid.String(),
		Gen:  gen.New_SB.gen,
		Root: gen.New_SB.root,
	}
	if err := gen.blockstore.db.C("superblocks").Insert(fsb); err != nil {
		lg.Panic(err)
	}
	gen.flushed = true

	gen.blockstore.glock.RLock()
	//log.Printf("bs is %v, wlocks is %v", gen.blockstore, gen.blockstore._wlocks)
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
	cblock.Identifier = gen.blockstore.allocateBlock()
	cblock.Generation = gen.Number()
	gen.cblocks = append(gen.cblocks, cblock)
	return cblock, nil
}

func (gen *Generation) AllocateVectorblock() (*Vectorblock, error) {
	vblock := &Vectorblock{}
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

func (bs *BlockStore) DEBUG_DELETE_UUID(id uuid.UUID) {
	lg.Info("DEBUG removing uuid '%v' from database", id.String())
	_, err := bs.db.C("superblocks").RemoveAll(bson.M{"uuid": id.String()})
	if err != nil && err != mgo.ErrNotFound {
		lg.Panic(err)
	}
	if err == mgo.ErrNotFound {
		lg.Info("Quey did not find supeblock to delete")
	} else {
		lg.Info("err was nik")
	}
	//bs.datablockBarrier()
}

func (bs *BlockStore) ReadDatablock(uuid uuid.UUID, addr uint64, impl_Generation uint64, impl_Pointwidth uint8, impl_StartTime int64) Datablock {
	//Try hit the cache first
	db := bs.cacheGet(addr)
	if db != nil {
		return db
	}
	syncbuf := block_buf_pool.Get().([]byte)
	trimbuf := bs.store.Read([]byte(uuid), addr, syncbuf)
	switch DatablockGetBufferType(trimbuf) {
	case Core:
		rv := &Coreblock{}
		rv.Deserialize(trimbuf)
		block_buf_pool.Put(syncbuf)
		rv.Identifier = addr
		rv.Generation = impl_Generation
		rv.PointWidth = impl_Pointwidth
		rv.StartTime = impl_StartTime
		bs.cachePut(addr, rv)
		return rv
	case Vector:
		rv := &Vectorblock{}
		rv.Deserialize(trimbuf)
		block_buf_pool.Put(syncbuf)
		rv.Identifier = addr
		rv.Generation = impl_Generation
		rv.PointWidth = impl_Pointwidth
		rv.StartTime = impl_StartTime
		bs.cachePut(addr, rv)
		return rv
	}
	lg.Panic("Strange datablock type")
	return nil
}

type fake_sblock struct {
	Uuid     string
	Gen      uint64
	Root     uint64
	Unlinked bool
}

// 从 MongoDB 中加载超级块信息
func (bs *BlockStore) LoadSuperblock(id uuid.UUID, generation uint64) *Superblock {
	var sb = fake_sblock{}
	if generation == LatestGeneration {
		//log.Info("loading superblock uuid=%v (lgen)", id.String())
		qry := bs.db.C("superblocks").Find(bson.M{"uuid": id.String()}) // 不指定 generation 那就按照该键排序，选出最新的来
		if err := qry.Sort("-gen").One(&sb); err != nil {
			if err == mgo.ErrNotFound {
				lg.Info("sb notfound!")
				return nil
			} else {
				lg.Panic(err)
			}
		}
	} else {
		qry := bs.db.C("superblocks").Find(bson.M{"uuid": id.String(), "gen": generation}) // 否则根据此得到一个结果
		if err := qry.One(&sb); err != nil {
			if err == mgo.ErrNotFound {
				return nil
			} else {
				lg.Panic(err)
			}
		}
	}
	rv := Superblock{
		uuid:     id,
		gen:      sb.Gen,
		root:     sb.Root,
		unlinked: sb.Unlinked,
	}
	return &rv
}

func CreateDatabase(params map[string]string) {
	ses, err := mgo.Dial(params["mongoserver"])
	if err != nil {
		lg.Critical("Could not connect to mongo database", err)
		os.Exit(1)
	}
	db := ses.DB(params["collection"])
	idx := mgo.Index{
		Key:        []string{"uuid", "-gen"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     false,
	}
	db.C("superblocks").EnsureIndex(idx)
	if err := os.MkdirAll(params["dbpath"], 0755); err != nil {
		lg.Panic(err)
	}
	fp := new(fileprovider.FileStorageProvider)
	err = fp.CreateDatabase(params)
	if err != nil {
		lg.Critical("Error on create: %v", err)
		os.Exit(1)
	}
}
