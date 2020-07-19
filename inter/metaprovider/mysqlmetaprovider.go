package metaprovider

import (
	"fmt"
	"github.com/jinzhu/gorm"
)
import _ "github.com/jinzhu/gorm/dialects/mysql"

type MySQLMetaProvider struct {
	db       *gorm.DB
	useCache bool
	cache    *metaCache
}

type meta struct {
	ID       uint   `gorm:"primary_key"`
	Uuid     string `gorm:"type:varchar(40);index:uuid_version;not null;"`
	Version  uint64 `gorm:"index:uuid_version;not null;"`
	Root     uint64
	Unlinked bool
}

func fromMeta(m *Meta) *meta {
	return &meta{
		Uuid:     m.Uuid,
		Version:  m.Version,
		Root:     m.Root,
		Unlinked: m.Unlinked,
	}
}

func toMeta(m *meta) *Meta {
	return &Meta{
		Uuid:     m.Uuid,
		Version:  m.Version,
		Root:     m.Root,
		Unlinked: m.Unlinked,
	}
}

func CreateMySQLMetaDatabase(params map[string]string) {
	db, err := gorm.Open("mysql", fmt.Sprintf("%s:%s@/%s?charset=utf8&parseTime=True&loc=Local",
		params["user"], params["password"], params["dbname"]))
	if err != nil {
		lg.Panicf("connect to mysql error: %v", err)
	}
	if db.HasTable(&meta{}) {
		db.DropTable(&meta{})
	}
	if err := db.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8").CreateTable(&meta{}).Error; err != nil {
		lg.Panicf("error in create table: %v", db.Error)
	}
}

func NewMySQLMetaProvider(params map[string]string) (*MySQLMetaProvider, error) {
	provider := &MySQLMetaProvider{}
	db, err := gorm.Open("mysql", fmt.Sprintf("%s:%s@/%s?charset=utf8&parseTime=True&loc=Local",
		params["user"], params["password"], params["dbname"]))
	if err != nil {
		return nil, err
	}
	db.DB().SetMaxOpenConns(300)
	db.DB().SetMaxIdleConns(200)
	provider.db = db
	provider.useCache = params["usecache"] == "true"
	if provider.useCache {
		provider.cache = newCache(16, 128, provider)
	}
	return provider, nil
}

func (p *MySQLMetaProvider) GetMeta(id string, version uint64) (*Meta, error) {
	if p.useCache {
		if m, found := p.cache.get(id, version); found {
			return m, nil
		}
	}
	m := &meta{}
	if err := p.db.First(&m, "uuid = ? AND version = ?", id, version).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, MetaNotFound
		}
		return nil, err
	}
	return toMeta(m), nil
}

func (p *MySQLMetaProvider) GetLatestMeta(id string) (*Meta, error) {
	if p.useCache {
		if m, found := p.cache.get(id, uint64(0)); found {
			return m, nil
		}
	}
	m := &meta{}
	if err := p.db.Where("uuid = ?", id).Order("version desc").First(m).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, MetaNotFound
		}
		return nil, err
	}
	return toMeta(m), nil
}

func (p *MySQLMetaProvider) InsertMeta(m *Meta) error {
	if p.useCache {
		if err := p.cache.set(m.Uuid, m.Version, m); err != nil {
			return err
		}
		return nil
	}
	if err := p.db.Create(fromMeta(m)).Error; err != nil {
		return err
	}
	return nil

}

func (m *MySQLMetaProvider) BatchInsertMeta(metaList []*Meta) error {
	sql := "INSERT INTO `meta` (`uuid`,`version`,`root`) VALUES "
	for i, met := range metaList {
		if i == len(metaList)-1 {
			sql += fmt.Sprintf("('%s', %d, %d);", met.Uuid, met.Version, met.Root)
		} else {
			sql += fmt.Sprintf("('%s', %d, %d),", met.Uuid, met.Version, met.Root)
		}
	}
	return m.db.Exec(sql).Error
}
