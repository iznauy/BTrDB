package bstore

import (
	"fmt"
	"github.com/op/go-logging"
	"os"
	"strconv"
)

var lg *logging.Logger

var (
	kfactor  = 64
	vsize    = 1024
	vbsize   = 2 + 9*vsize + 9*vsize + 2*vsize
	dbsize   = vbsize
	pwfactor = uint8(6)
)

func init() {
	lg = logging.MustGetLogger("log")
	k, err := strconv.Atoi(os.Getenv("K"))
	if err != nil {
		lg.Fatal("无法从环境变量中读取 K")
		return
	}
	fmt.Printf("从环境变量中读取了 K 的值，K = %d\n", k)
	kfactor = k
	pw := 0
	for k > 1 {
		k /= 2
		pw += 1
	}
	pwfactor = uint8(pw)
}

//Note to self, if you bump VSIZE such that the max blob goes past 2^16, make sure to adapt
//providers
const (
	RELOCATION_BASE = 0xFF00000000000000
)

func GetVSize() int {
	return vsize
}

func GetKFactor() int {
	return kfactor
}

func GetDBSize() int {
	return dbsize
}

func GetPWFactor() uint8 {
	return pwfactor
}
