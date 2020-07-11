package grpcinterface

import (
	"context"
	btrdb2 "github.com/iznauy/BTrDB/btrdbd"
	"github.com/iznauy/BTrDB/qtree"
	"github.com/op/go-logging"
	"github.com/pborman/uuid"
	"google.golang.org/grpc"
	"net"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

var Success = &Status{
	Code: 0,
	Msg:  "",
}

var ErrBadUUID = &Status{
	Code: 400,
	Msg:  "Invalid uuid",
}

var ErrBadTimes = &Status{
	Code: 402,
	Msg:  "Invalid time range",
}

type GRPCInterface struct {
	q *btrdb2.Quasar
}

func ServeGRPC(q *btrdb2.Quasar, addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()
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
		return nil, nil
	}
	return &QueryNearestValueResponse{
		Status:Success,
		Version: version,
		Value: &RawPoint{
			Time: record.Time,
			Value: record.Val,
		},
	}, nil
}

func checkTime(time int64) bool {
	return time >= btrdb2.MinimumTime && time < btrdb2.MaximumTime
}
