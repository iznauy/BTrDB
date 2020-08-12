package grpcinterface

import (
	"context"
	"errors"
	"fmt"
	btrdb2 "github.com/iznauy/BTrDB/btrdbd"
	"github.com/iznauy/BTrDB/qtree"
	"github.com/op/go-logging"
	"github.com/pborman/uuid"
	"google.golang.org/grpc"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

var batchInsertInProcess int32 = 0

var (
	span time.Duration
	times int64
	mu sync.Mutex
)

var Success = &Status{
	Code: 0,
	Msg:  "",
}

var ErrBadUUID = &Status{
	Code: 400,
	Msg:  "Invalid uuid",
}

var BadUUID = errors.New("Invalid uuid")

var ErrBadTimes = &Status{
	Code: 402,
	Msg:  "Invalid time range",
}

var BadTimes = errors.New("Invalid time range")

type GRPCInterface struct {
	q *btrdb2.Quasar
}

func ServeGRPC(q *btrdb2.Quasar, addr string) {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			fmt.Println("Num BatchInsert In Process: ", batchInsertInProcess)
		}
	}()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	maxSize := 200 * 1024 * 1024 // 最大消息为 40M，这样一次可以传输上百万个数据点
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(maxSize), grpc.MaxSendMsgSize(maxSize))
	RegisterBTrDBServer(grpcServer, &GRPCInterface{
		q: q,
	})
	if err := grpcServer.Serve(l); err != nil {
		log.Fatalf("fail to serve: %v", err)
	}
}

func (g *GRPCInterface) Insert(ctx context.Context, req *InsertRequest) (*InsertResponse, error) {
	records := make([]qtree.Record, 0, len(req.Values))
	for _, val := range req.Values {
		if val == nil {
			continue
		}
		record := qtree.Record{
			Time: val.Time,
			Val:  val.Value,
		}
		if !checkTime(record.Time) {
			return &InsertResponse{
				Status: ErrBadTimes,
			}, nil
		}
		records = append(records, record)
	}
	id, err := uuid.ParseBytes(req.Uuid)
	if err != nil {
		log.Fatal("[Insert] invalid uuid: %v", err)
		return &InsertResponse{
			Status: ErrBadUUID,
		}, nil
	}
	g.q.InsertValues(id, records)
	return &InsertResponse{
		Status: Success,
	}, nil
}

func (g *GRPCInterface) batchInsert(insertReqs []*InsertRequest, w *sync.WaitGroup) { // 同步方法
	recordsMap := make(map[string][]qtree.Record, len(insertReqs))
	for _, req := range insertReqs {
		records := make([]qtree.Record, 0, len(req.Values))
		for _, val := range req.Values {
			if val == nil {
				continue
			}
			record := qtree.Record{
				Time: val.Time,
				Val:  val.Value,
			}
			records = append(records, record)
		}
		recordsMap[string(req.Uuid)] = records
	}
	for uid, records := range recordsMap {
		id := uuid.Parse(uid)
		start := time.Now()
		g.q.InsertValues(id, records)
		localSpan := time.Now().Sub(start)
		mu.Lock()
		span += localSpan
		times += 1
		if times % 1000 == 0 {
			fmt.Println("最近1000次数据点插入，平均每次耗时：", float64(span.Milliseconds()) / 1000.0, "ms")
			times = 0
			span = 0
		}
		mu.Unlock()
	}
	w.Done()
}

func (g *GRPCInterface) BatchInsert(ctx context.Context, batchReq *BatchInsertRequest) (*BatchInsertResponse, error) {
	// 首先对数据进行一次预检，有问题及时返回
	for _, insertReq := range batchReq.Inserts {
		err := checkInsertReq(insertReq)
		if err != nil {
			if err == BadTimes {
				return &BatchInsertResponse{
					Status: ErrBadTimes,
				}, nil
			} else if err == BadUUID {
				return &BatchInsertResponse{
					Status: ErrBadUUID,
				}, nil
			}
			return &BatchInsertResponse{ // 理论上不应该走到这儿
				Status: &Status{
					Code: -1,
					Msg:  err.Error(),
				},
			}, nil
		}
	}

	batchCount := len(batchReq.Inserts)
	batches := make([][]*InsertRequest, batchCount)

	for i := 0; i < batchCount; i++ {
		from := i
		to := from + 1
		if to > len(batchReq.Inserts) {
			to = len(batchReq.Inserts)
		}
		batches = append(batches, batchReq.Inserts[from:to])
	}

	atomic.AddInt32(&batchInsertInProcess, 1)
	var w sync.WaitGroup
	for _, batch := range batches {
		w.Add(1)
		go g.batchInsert(batch, &w)
	}
	w.Wait()
	atomic.AddInt32(&batchInsertInProcess, -1)

	return &BatchInsertResponse{
		Status: Success,
	}, nil
}

func (g *GRPCInterface) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	id, err := uuid.ParseBytes(req.Uuid)
	if err != nil {
		log.Fatal("[Delete] invalid uuid: %v", err)
		return &DeleteResponse{
			Status: ErrBadUUID,
		}, nil
	}
	err = g.q.DeleteRange(id, req.Start, req.End)
	if err != nil {
		return &DeleteResponse{
			Status: &Status{
				Code: -1, // TODO: 新增错误表
				Msg:  err.Error(),
			},
		}, nil
	}
	return &DeleteResponse{
		Status: Success,
	}, nil
}

func (g *GRPCInterface) QueryRange(ctx context.Context, req *QueryRangeRequest) (*QueryRangeResponse, error) {
	id, err := uuid.ParseBytes(req.Uuid)
	if err != nil {
		log.Fatal("[QueryRange] invalid uuid: %v", err)
		return &QueryRangeResponse{
			Status: ErrBadUUID,
		}, nil
	}
	version := req.Version
	if version == 0 {
		version = btrdb2.LatestGeneration
	}
	records, version, err := g.q.QueryValues(id, req.Start, req.End, version)
	if err != nil {
		return &QueryRangeResponse{
			Status: &Status{
				Code: -1,
				Msg:  err.Error(),
			},
		}, nil
	}
	values := make([]*RawPoint, len(records))
	for i := 0; i < len(records); i++ {
		values[i] = &RawPoint{
			Time:  records[i].Time,
			Value: records[i].Val,
		}
	}
	return &QueryRangeResponse{
		Status:  Success,
		Version: version,
		Values:  values,
	}, nil
}

func (g *GRPCInterface) QueryNearestValue(ctx context.Context, req *QueryNearestValueRequest) (*QueryNearestValueResponse, error) {
	log.Infof("[QueryNearestValue] req=%v", req)
	id, err := uuid.ParseBytes(req.Uuid)
	if err != nil {
		log.Fatal("[QueryNearestValue] invalid uuid: %v", err)
		return &QueryNearestValueResponse{
			Status: ErrBadUUID,
		}, nil
	}
	version := req.Version
	if version == 0 {
		version = btrdb2.LatestGeneration
	}
	record, version, err := g.q.QueryNearestValue(id, req.Time, req.Backwards, version)
	if err != nil {
		return &QueryNearestValueResponse{
			Status: &Status{
				Code: -1,
				Msg:  err.Error(),
			},
		}, nil
	}
	return &QueryNearestValueResponse{
		Status:  Success,
		Version: version,
		Value: &RawPoint{
			Time:  record.Time,
			Value: record.Val,
		},
	}, nil
}

func (g *GRPCInterface) QueryStatistics(ctx context.Context, req *QueryStatisticsRequest) (*QueryStatisticsResponse, error) {
	log.Infof("[QueryStatistics] req=%v", req)
	id, err := uuid.ParseBytes(req.Uuid)
	if err != nil {
		log.Fatalf("[QueryStatistics] invalid uuid: %v, len(uuid)=%d", err, len(req.Uuid))
		return &QueryStatisticsResponse{
			Status: ErrBadUUID,
		}, nil
	}
	version := req.Version
	if version == 0 {
		version = btrdb2.LatestGeneration
	}
	records, version, err := g.q.QueryStatisticalValues(id, req.Start, req.End, version, uint8(req.Resolution))
	if err != nil {
		return &QueryStatisticsResponse{
			Status: &Status{
				Code: -1,
				Msg:  err.Error(),
			},
		}, nil
	}
	statistics := make([]*Statistics, 0, len(records))
	for _, record := range records {
		statistic := &Statistics{
			Start: record.Time,
			End:   record.Time + (1 << req.Resolution),
			Max:   record.Max,
			Min:   record.Min,
			Mean:  record.Mean,
		}
		statistics = append(statistics, statistic)
	}
	return &QueryStatisticsResponse{
		Status:     Success,
		Version:    version,
		Statistics: statistics,
	}, nil
}

func checkTime(time int64) bool {
	return time >= btrdb2.MinimumTime && time < btrdb2.MaximumTime
}

func checkInsertReq(req *InsertRequest) error {
	_, err := uuid.ParseBytes(req.Uuid)
	if err != nil {
		log.Fatal("[checkInsertReq] invalid uuid: %v", err)
		return BadUUID
	}
	for _, point := range req.Values {
		if !checkTime(point.Time) {
			log.Fatal("[checkInsertReq] invalid time: %v", point)
			return BadTimes
		}
	}
	return nil
}
