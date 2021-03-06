package conf

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/gcfg.v1"
)

type Config struct {
	GRPC struct {
		Port           *int
		Address        *string
		Enabled        bool
		UseRateLimiter bool
		ReadLimit      *int
		WriteLimit     *int
		LimitVariable  *bool
	}
	Meta struct {
		Provider          string
		MongoDBServer     *string
		MongoDBCollection *string
		MySQLUser         *string
		MySQLPassword     *string
		MySQLDBName       *string
		UseCache          *string
	}
	Storage struct {
		Filepath *string
	}
	Cache struct {
		BlockCache int
	}
	Debug struct {
		Cpuprofile  bool
		Heapprofile bool
	}
	Coalescence struct {
		Enable    bool
		Earlytrip *int
		Interval  *int
	}
	Forest struct {
		Count *int
	}
}

var Configuration Config
var Params map[string]string

func LoadConfig() {
	found := false
	err := gcfg.ReadFileInto(&Configuration, "./btrdb.conf")
	if err != nil {
		fmt.Printf("Could not load configuration file './btrdb.conf': %v\n", err)
	} else {
		found = true
	}

	if cacheSize, err := strconv.Atoi(os.Getenv("CacheSize")); err == nil {
		fmt.Println("从环境变量中读取了 cache 的值，cache 的大小为", cacheSize)
		Configuration.Cache.BlockCache = cacheSize
	}

	if !found {
		err := gcfg.ReadFileInto(&Configuration, "/etc/btrdbd/btrdb.conf")
		if err != nil {
			fmt.Printf("Could not load configuration file '/etc/btrdbd/btrdb.conf':\n%v\n", err)
		} else {
			found = true
		}
	}

	if !found {
		fmt.Printf("Aborting: no configuration found!\n")
		os.Exit(1)
	}

	if Configuration.Meta.Provider == "mysql" {
		if Configuration.Meta.MySQLUser == nil {
			fmt.Printf("Aborting: configuration missing MySQL user name\n")
			os.Exit(1)
		}
		if Configuration.Meta.MySQLPassword == nil {
			fmt.Printf("Aborting: configuration missing MySQL password\n")
			os.Exit(1)
		}
		if Configuration.Meta.MySQLDBName == nil {
			fmt.Printf("Aborting: configuration missing MySQL database name\n")
			os.Exit(1)
		}
	} else if Configuration.Meta.Provider == "mongodb" {
		if Configuration.Meta.MongoDBServer == nil {
			fmt.Printf("Aborting: configuration missing MongoDB server address\n")
			os.Exit(1)
		}
		if Configuration.Meta.MongoDBCollection == nil {
			fmt.Printf("Aborting: configuration missing MongoDB collection\n")
			os.Exit(1)
		}
	} else if Configuration.Meta.Provider == "mem" {

	} else {
		fmt.Printf("Aborting: unknown meta provider specified\n")
		os.Exit(1)
	}

	if Configuration.GRPC.Enabled && Configuration.GRPC.Port == nil {
		fmt.Printf("Aborting: grpc server enabled, but no port specified\n")
		os.Exit(1)
	}

	if Configuration.GRPC.Enabled && Configuration.GRPC.Address == nil {
		fmt.Printf("Aborting: grpc server enabled, but no address specified\n")
		os.Exit(1)
	}

	if Configuration.GRPC.UseRateLimiter && Configuration.GRPC.ReadLimit == nil {
		fmt.Printf("Aborting: grpc server use rate limiter, but no read limiter specified\n")
		os.Exit(1)
	}

	if Configuration.GRPC.UseRateLimiter && Configuration.GRPC.WriteLimit == nil {
		fmt.Printf("Aborting: grpc server use rate limiter, but no write limiter specified\n")
		os.Exit(1)
	}

	if Configuration.GRPC.UseRateLimiter && Configuration.GRPC.LimitVariable == nil {
		fmt.Printf("Aborting: grpc server use rate limiter, but limiter variable not set\n")
		os.Exit(1)
	}

	if Configuration.Coalescence.Enable && Configuration.Coalescence.Earlytrip == nil {
		fmt.Printf("Aborting: transaction coalescence early trip object count not set\n")
		os.Exit(1)
	}

	if Configuration.Coalescence.Enable && Configuration.Coalescence.Interval == nil {
		fmt.Printf("Aborting: transaction coalescence commit interval not set\n")
		os.Exit(1)
	}

	if Configuration.Forest.Count == nil {
		fmt.Printf("Aborting: forest count not set\n")
		os.Exit(1)
	} else if *Configuration.Forest.Count <= 0 {
		fmt.Printf("Aborting: forest count must be positive number\n")
		os.Exit(1)
	}

	Params = map[string]string{
		"cachesize":    strconv.FormatInt(int64(Configuration.Cache.BlockCache), 10),
		"metaprovider": Configuration.Meta.Provider,
		"useCache":     *Configuration.Meta.UseCache,
		"dbpath":       *Configuration.Storage.Filepath,
	}

	if Configuration.Meta.Provider == "mysql" {
		Params["user"] = *Configuration.Meta.MySQLUser
		Params["password"] = *Configuration.Meta.MySQLPassword
		Params["dbname"] = *Configuration.Meta.MySQLDBName
	}
	if Configuration.Meta.Provider == "mongodb" {
		Params["server"] = *Configuration.Meta.MongoDBServer
		Params["collection"] = *Configuration.Meta.MongoDBCollection
	}

	fmt.Printf("Configuration OK!\n")
}
