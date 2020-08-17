package bstore

import (
	"github.com/op/go-logging"
)

var lg *logging.Logger

//Note to self, if you bump VSIZE such that the max blob goes past 2^16, make sure to adapt
//providers
const (
	RELOCATION_BASE = 0xFF00000000000000
)
