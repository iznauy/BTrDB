package grpcinterface

import (
	"context"
	btrdb2 "github.com/iznauy/BTrDB/btrdbd"
	"github.com/op/go-logging"
	"google.golang.org/grpc"
	"net"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
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

func (g *GRPCInterface) TestConnection(ctx context.Context, req *TestConnectionRequest) (*TestConnectionResponse, error) {
	return &TestConnectionResponse{Success: true}, nil
}
