package fileprovider

import (
	"fmt"
	"github.com/iznauy/BTrDB/inter/bprovider"
	"github.com/op/go-logging"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

var (
	span  time.Duration
	times int64
	mu    sync.Mutex
)

const NUMFILES = 256

type writeparams struct {
	Address uint64
	Data    []byte
}

type FileProviderSegment struct {
	sp    *FileStorageProvider
	fidx  int
	f     *os.File
	base  int64
	ptr   int64
	wchan chan writeparams
	wg    sync.WaitGroup
}

type FileStorageProvider struct {
	fidx     chan int
	retfidx  chan int
	dbf      []*os.File
	dbrf     []*os.File
	dbrf_mtx []sync.Mutex
	favail   []bool
}

func (seg *FileProviderSegment) writer() {

	for args := range seg.wchan {
		off := int64(args.Address & ((1 << 50) - 1))
		lenarr := make([]byte, 2)
		lenarr[0] = byte(len(args.Data))
		lenarr[1] = byte(len(args.Data) >> 8)
		_, err := seg.f.WriteAt(lenarr, off)
		if err != nil {
			log.Panic("File writing error %v", err)
		}
		_, err = seg.f.WriteAt(args.Data, off+2)
		if err != nil {
			log.Panic("File writing error %v", err)
		}
	}
	seg.wg.Done()
}

func (seg *FileProviderSegment) init() {
	seg.wchan = make(chan writeparams, 16)
	seg.wg.Add(1)
	go seg.writer()
}

//Returns the address of the first free word in the segment when it was locked
func (seg *FileProviderSegment) BaseAddress() uint64 {
	//This seems arbitrary, why not go with the top 8 bits? The reason is this:
	//a) this still leaves 1PB per file
	//b) The huffman encoding can do 58 bits in 8 bytes, but anything more is 9
	//c) if we later decide to more than 256 files, we can
	return (uint64(seg.fidx) << 50) + uint64(seg.base)
}

//Unlocks the segment for the StorageProvider to give to other consumers
//Implies a flush
func (seg *FileProviderSegment) Unlock() {
	seg.Flush()
	seg.sp.retfidx <- seg.fidx
}

//Writes a slice to the segment, returns immediately
//Returns nil if op is OK, otherwise ErrNoSpace or ErrInvalidArgument
//It is up to the implementer to work out how to report no space immediately
//The uint64 rv is the address to be used for the next write
func (seg *FileProviderSegment) Write(uuid []byte, address uint64, data []byte) (uint64, error) {
	//TODO remove
	if seg.ptr != int64(address&((1<<50)-1)) {
		log.Panic("Pointer does not match address %x vs %x", seg.ptr, int64(address&((1<<50)-1)))
	}
	wp := writeparams{Address: address, Data: data}
	seg.wchan <- wp
	seg.ptr = int64(address&((1<<50)-1)) + int64(len(data)) + 2
	return uint64(seg.ptr) + (uint64(seg.fidx) << 50), nil
}

//Block until all writes are complete, not
func (seg *FileProviderSegment) Flush() {
	close(seg.wchan)
	seg.wg.Wait()
}

// provideFiles 高性能实现
func (sp *FileStorageProvider) provideFilesV2() {
	available := NUMFILES
	for {
	loop:
		for {
			select {
			case fi := <-sp.retfidx:
				sp.favail[fi] = true
				available += 1
			default:
				break loop
			}
		}
		base := rand.Intn(NUMFILES)
		for i := 0; i < NUMFILES; i++ {
			if !sp.favail[(base+i)%NUMFILES] {
				continue
			}
			available -= 1
			sp.favail[(base+i)%NUMFILES] = false
			sp.fidx <- (base + i) % NUMFILES
		inner:
			for {
				select {
				case fi := <-sp.retfidx:
					sp.favail[fi] = true
					available += 1
				default:
					break inner
				}
			}
		}
		if available < NUMFILES {
			fi := <-sp.retfidx
			sp.favail[fi] = true
			available += 1
		}
	}
}

//Provide file indices into fidx, does not return
func (sp *FileStorageProvider) provideFiles() {
	for {
		//Read all returned files
	ldretfi:
		for {
			select {
			case fi := <-sp.retfidx: // 还回来的文件，可以提供 segment
				sp.favail[fi] = true // favail = file available now
			default: // 假如没有归还的 file，那么就跳出循环
				break ldretfi
			}
		}

		//Greedily select file
		minidx := -1
		var minv int64 = 0
		for i := 0; i < NUMFILES; i++ {
			if !sp.favail[i] {
				continue
			}
			off, err := sp.dbf[i].Seek(0, os.SEEK_CUR)
			if err != nil {
				log.Panic(err)
			}
			if minidx == -1 || off < minv {
				minidx = i
				minv = off
			}
		}

		//Return it, or do blocking read if not found
		if minidx != -1 { // 找一个数据量最小的文件，这样可以保证每个文件的大小都差不多。
			sp.favail[minidx] = false
			sp.fidx <- minidx
		} else {
			//Do a blocking read on retfidx to avoid fast spin on nonblocking
			// 干得漂亮
			fi := <-sp.retfidx
			sp.favail[fi] = true
		}

	}
}

//Called at startup
func (sp *FileStorageProvider) Initialize(opts map[string]string) {
	//Initialize file indices thingy
	sp.fidx = make(chan int)
	sp.retfidx = make(chan int, NUMFILES+1)
	sp.dbf = make([]*os.File, NUMFILES)
	sp.dbrf = make([]*os.File, NUMFILES)
	sp.dbrf_mtx = make([]sync.Mutex, NUMFILES)
	sp.favail = make([]bool, NUMFILES)
	for i := 0; i < NUMFILES; i++ {
		//Open file
		dbpath, ok := opts["dbpath"]
		if !ok {
			log.Panic("Expected dbpath")
		}
		fname := fmt.Sprintf("%s/blockstore.%02x.db", dbpath, i)
		//write file descriptor
		{
			f, err := os.OpenFile(fname, os.O_RDWR, 0666)
			if err != nil && os.IsNotExist(err) {
				log.Critical("Aborting: seems database does not exist. Have you run `btrdbd -makedb`?")
				os.Exit(1)
			}
			if err != nil {
				log.Panicf("Problem with blockstore DB: ", err)
			}
			sp.dbf[i] = f
		}
		//Read file descriptor
		{
			f, err := os.OpenFile(fname, os.O_RDONLY, 0666)
			if err != nil {
				log.Panicf("Problem with blockstore DB: ", err)
			}
			sp.dbrf[i] = f
		}
		sp.favail[i] = true
	}
	go sp.provideFilesV2()

}

// Lock a segment, or block until a segment can be locked
// Returns a Segment struct
func (sp *FileStorageProvider) LockSegment(uuid []byte) bprovider.Segment {
	//Grab a file index
	start := time.Now()
	fidx := <-sp.fidx
	localSpan := time.Now().Sub(start)
	mu.Lock()
	span += localSpan
	times++
	if times%1000 == 0 {
		fmt.Println("最近1000次获取文件锁，平均每次耗时：", float64(span.Milliseconds())/1000.0, "ms")
		times = 0
		span = 0
	}
	mu.Unlock()
	f := sp.dbf[fidx]
	l, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		log.Panicf("Error on lock segment: %v", err)
	}

	//Construct segment
	seg := &FileProviderSegment{sp: sp, fidx: fidx, f: sp.dbf[fidx], base: l, ptr: l}
	seg.init()

	return seg
}

//This is the size of a maximal size cblock + header
const FIRSTREAD = 3459

func (sp *FileStorageProvider) Read(uuid []byte, address uint64, buffer []byte) []byte {
	fidx := address >> 50
	off := int64(address & ((1 << 50) - 1))
	if fidx > NUMFILES {
		log.Panic("Encoded file idx too large")
	}
	sp.dbrf_mtx[fidx].Lock()
	nread, err := sp.dbrf[fidx].ReadAt(buffer[:FIRSTREAD], off)
	if err != nil && err != io.EOF {
		log.Panic("Non EOF read error: %v", err)
	}
	if nread < 2 { // 前两个字节描述了数据块的长度，读取的数据小于2字节说明出了问题
		log.Panic("Unexpected (very) short read")
	}
	//Now we read the blob size
	bsize := int(buffer[0]) + (int(buffer[1]) << 8)
	if bsize > nread-2 { // 针对 core block 不会出现这种情况，但是针对 vector block 有可能出现，因此再读一次
		_, err := sp.dbrf[fidx].ReadAt(buffer[nread:bsize+2], off+int64(nread))
		if err != nil {
			log.Panic("Read error: %v", err)
		}
	}
	sp.dbrf_mtx[fidx].Unlock()
	return buffer[2 : bsize+2]
}

//Called to create the database for the first time
func (sp *FileStorageProvider) CreateDatabase(opts map[string]string) error {
	for i := 0; i < NUMFILES; i++ {
		//Open file
		dbpath, ok := opts["dbpath"]
		if !ok {
			log.Panicf("Expected dbpath")
		}
		fname := fmt.Sprintf("%s/blockstore.%02x.db", dbpath, i)
		//write file descriptor
		{
			f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
			if err != nil && !os.IsExist(err) {
				log.Panicf("Problem with blockstore DB: ", err)
			} else if os.IsExist(err) {
				return bprovider.ErrExists
			}
			//Add a file tag
			//An exercise left for the reader: if you remove this, everything breaks :-)
			//Hint: what is the physical address of the first byte of file zero?
			_, err = f.Write([]byte("QUASARDB"))
			if err != nil {
				log.Panicf("Could not write to blockstore:", err)
			}

			err = f.Close()
			if err != nil {
				log.Panicf("Error on close %v", err)
			}
		}
	}
	return nil
}
