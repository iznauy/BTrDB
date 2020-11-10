package main

import (
	"flag"
	"fmt"
	"github.com/iznauy/BTrDB/brain/handler"
	"github.com/iznauy/BTrDB/btrdbd"
	"github.com/iznauy/BTrDB/conf"
	"github.com/iznauy/BTrDB/grpcinterface"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/iznauy/BTrDB/inter/bstore"
	"github.com/op/go-logging"
)

var log *logging.Logger

func init() {
	logging.SetFormatter(logging.MustStringFormatter("%{color}%{shortfile} ▶%{color:reset} %{message}"))
	log = logging.MustGetLogger("log")

}

var createDB = flag.Bool("makedb", false, "create a new database")

func main() {
	conf.LoadConfig()
	flag.Parse()

	go func() {
		for {
			time.Sleep(10 * time.Second)
			fmt.Println("Num goroutines: ", runtime.NumGoroutine())
		}
	}()
	if conf.Configuration.Debug.Cpuprofile {
		f, err := os.Create("profile.cpu")
		if err != nil {
			log.Panicf("Error creating CPU profile: %v", err)
		}
		f2, err := os.Create("profile.block")
		if err != nil {
			log.Panicf("Error creating Block profile: %v", err)
		}
		pprof.StartCPUProfile(f)
		runtime.SetBlockProfileRate(1)
		defer runtime.SetBlockProfileRate(0)
		defer pprof.Lookup("block").WriteTo(f2, 1)
		defer pprof.StopCPUProfile()
	}

	if *createDB {
		fmt.Printf("Creating a new database\n")
		bstore.CreateDatabase(conf.Params)
		fmt.Printf("Done\n")
		os.Exit(0)
	}
	handler.RegisterEventHandlers()
	nCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nCPU)
	cfg := btrdbd.QuasarConfig{
		DatablockCacheSize:        uint64(conf.Configuration.Cache.BlockCache),
		TransactionCoalesceEnable: conf.Configuration.Coalescence.Enable,
		ForestCount:               uint64(*conf.Configuration.Forest.Count),
		Params:                    conf.Params,
	}
	if conf.Configuration.Coalescence.Enable {
		cfg.TransactionCoalesceInterval = uint64(*conf.Configuration.Coalescence.Interval)
		cfg.TransactionCoalesceEarlyTrip = uint64(*conf.Configuration.Coalescence.Earlytrip)
	}
	q, err := btrdbd.NewQuasar(&cfg)
	if err != nil {
		log.Panicf("error: ", err)
	}

	if conf.Configuration.GRPC.Enabled {
		grpcConfig := grpcinterface.GrpcConfig{
			Address:        *conf.Configuration.GRPC.Address + ":" + strconv.FormatInt(int64(*conf.Configuration.GRPC.Port), 10),
			UseRateLimiter: conf.Configuration.GRPC.UseRateLimiter,
		}
		if grpcConfig.UseRateLimiter {
			grpcConfig.LimitVariable = *conf.Configuration.GRPC.LimitVariable
			grpcConfig.ReadLimit = int64(*conf.Configuration.GRPC.ReadLimit)
			grpcConfig.WriteLimit = int64(*conf.Configuration.GRPC.WriteLimit)
		}
		go grpcinterface.ServeGRPC(q, &grpcConfig)
	}

	if conf.Configuration.Debug.Heapprofile {
		go func() {
			idx := 0
			for {
				f, err := os.Create(fmt.Sprintf("profile.heap.%05d", idx))
				if err != nil {
					log.Panicf("Could not create memory profile %v", err)
				}
				idx = idx + 1
				pprof.WriteHeapProfile(f)
				f.Close()
				time.Sleep(30 * time.Second)
			}
		}()
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	for {
		time.Sleep(5 * time.Second)
		log.Info("Still alive")

		select {
		case _ = <-sigchan:
			log.Warning("Received Ctrl-C, waiting for graceful shutdown")
			time.Sleep(4 * time.Second) //Allow http some time
			log.Warning("Checking for pending inserts")
			for {
				if q.IsPending() {
					log.Warning("Pending inserts... waiting... ")
					time.Sleep(2 * time.Second)
				} else {
					log.Warning("No pending inserts")
					break
				}
			}
			if conf.Configuration.Debug.Heapprofile {
				log.Warning("writing heap profile")
				f, err := os.Create("profile.heap.FIN")
				if err != nil {
					log.Panicf("Could not create memory profile %v", err)
				}
				pprof.WriteHeapProfile(f)
				f.Close()

			}
			return //end the program
		default:

		}
	}
}
