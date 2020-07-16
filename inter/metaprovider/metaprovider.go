package metaprovider

import (
	"errors"
	"github.com/op/go-logging"
)

var MetaNotFound = errors.New("meta not found")

var lg *logging.Logger

func init() {
	lg = logging.MustGetLogger("log")
}

type Meta struct {
	Uuid     string
	Version  uint64
	Root     uint64
	Unlinked bool
}

type MetaProvider interface {
	GetMeta(id string, version uint64) (*Meta, error)
	GetLatestMeta(id string) (*Meta, error)
	InsertMeta(meta *Meta) error
}

type BatchInsertMetaProvider interface {
	MetaProvider
	BatchInsertMeta(metaList []*Meta) error
}
