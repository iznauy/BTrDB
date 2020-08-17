package grpcinterface

import (
	"github.com/juju/ratelimit"
	"github.com/yangwenmai/ratelimit/leakybucket"
	"sync"
	"sync/atomic"
	"time"
)

type GrpcReadRequest interface {
	readBytes() int64 // 读请求实际需要预估请求大小
}

type GrpcWriteRequest interface {
	writeBytes() int64
}

type RateLimiter interface {
	Read(req GrpcReadRequest) bool
	Write(req GrpcWriteRequest) bool
}

type rateLimiter struct {
	readLimiter int64
	readBytes   int64
	readBucket  *ratelimit.Bucket

	writeLimiter int64
	writeBytes   int64
	writeBucket  leakybucket.BucketI

	variable bool // 限流是否跟随系统负载动态变化
	mu       sync.RWMutex
}

func newLimiter(readLimiter, writeLimiter int64, variable bool) *rateLimiter {
	r := &rateLimiter{}

	r.readLimiter = readLimiter
	r.writeLimiter = writeLimiter
	r.variable = variable

	r.readBucket = ratelimit.NewBucketWithRate(float64(readLimiter), 5*readLimiter)
	writeBucket, _ := leakybucket.New().Create("", uint(writeLimiter), time.Second)
	r.writeBucket = writeBucket

	if variable {
		go r.vary()
	}

	return r
}

func (r *rateLimiter) vary() {
	for {
		time.Sleep(30 * time.Second)

		// 访问决策模块获取更新的限速速率，暂时是 mock 程序
		r.readLimiter, r.writeLimiter = r.nextReadWriteLimiter()

		// 变更限流速率
		r.mu.Lock()
		r.readBucket = ratelimit.NewBucketWithRate(float64(r.readLimiter), 5*r.readLimiter)
		writeBucket, _ := leakybucket.New().Create("", uint(r.writeLimiter), time.Second)
		r.writeBucket = writeBucket
		r.mu.Unlock()

		// 原子操作清空统计信息
		_ = atomic.SwapInt64(&r.readBytes, 0)
		_ = atomic.SwapInt64(&r.writeBytes, 0)
	}
}

func (r *rateLimiter) nextReadWriteLimiter() (int64, int64) { // TODO: 添加策略
	return r.readLimiter, r.writeLimiter
}

func (r *rateLimiter) Read(req GrpcReadRequest) bool {
	bytes := req.readBytes()

	// 更新统计信息，而且访问 bucket 需要加读锁
	if r.variable {
		atomic.AddInt64(&r.readBytes, bytes)
		r.mu.RLock()
		defer r.mu.RUnlock()
	}

	if _, ok := r.readBucket.TakeMaxDuration(bytes, time.Second); !ok {
		return false
	}
	return true
}

func (r *rateLimiter) Write(req GrpcWriteRequest) bool {
	bytes := req.writeBytes()

	// 更新统计信息，而且访问 bucket 需要加读锁
	if r.variable {
		atomic.AddInt64(&r.writeBytes, bytes)
		r.mu.RLock()
		defer r.mu.RUnlock()
	}

	_, err := r.writeBucket.Add(uint(bytes))
	if err != nil {
		return false
	}
	return true
}

func (req *InsertRequest) writeBytes() int64 {
	return int64(len(req.Uuid) + len(req.Values)*24) // 24 = 8（指针大小）+ 8（时间戳大小）+ 8（数据大小）
}

func (req *BatchInsertRequest) writeBytes() int64 {
	bytes := int64(0)
	for _, insertReq := range req.Inserts {
		bytes += insertReq.writeBytes()
	}
	return bytes + int64(len(req.Inserts)*8)
}

func (req *DeleteRequest) writeBytes() int64 {
	return int64(len(req.Uuid) + 16)
}

func (req *QueryRangeRequest) readBytes() int64 {
	return int64(len(req.Uuid) + 24)
}

func (req *QueryStatisticsRequest) readBytes() int64 {
	return int64(len(req.Uuid) + 28)
}

func (req *QueryNearestValueRequest) readBytes() int64 {
	return int64(len(req.Uuid) + 17)
}
