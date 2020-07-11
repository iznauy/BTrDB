package main

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/gcfg.v1"
)

type Config struct {
	GRPC struct {
		Port    *int
		Address *string
		Enabled bool
	}
	Meta struct {
		Provider          string
		MongoDBServer     *string
		MongoDBCollection *string
		MySQLUser         *string
		MySQLPassword     *string
		MySQLDBName       *string
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
		Earlytrip *int
		Interval  *int
	}
}

var Configuration Config
var Params map[string]string

func loadConfig() {
	found := false
	err := gcfg.ReadFileInto(&Configuration, "./btrdb.conf")
	if err != nil {
		fmt.Printf("Could not load configuration file './btrdb.conf':\n%v\n", err)
	} else {
		found = true
	}

	if !found {
		err := gcfg.ReadFileInto(&Configuration, "/etc/btrdbd/btrdbd.conf")
		if err != nil {
			fmt.Printf("Could not load configuration file '/etc/btrdbd/btrdbd.conf':\n%v\n", err)
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

	if Configuration.Coalescence.Earlytrip == nil {
		fmt.Printf("Aborting: transaction coalescence early trip object count not set\n")
		os.Exit(1)
	}

	if Configuration.Coalescence.Interval == nil {
		fmt.Printf("Aborting: transaction coalescence commit interval not set\n")
		os.Exit(1)
	}

	Params = map[string]string{
		"cachesize":    strconv.FormatInt(int64(Configuration.Cache.BlockCache), 10),
		"metaprovider": Configuration.Meta.Provider,
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
