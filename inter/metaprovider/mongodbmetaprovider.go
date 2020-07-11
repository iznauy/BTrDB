package metaprovider

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
)

type MongoDBMetaProvider struct {
	ses *mgo.Session
	db  *mgo.Database
}

func CreateMongoDBMetaDatabase(params map[string]string) {
	ses, err := mgo.Dial(params["server"])
	if err != nil {
		lg.Critical("Could not connect to mongo database", err)
		os.Exit(1)
	}
	db := ses.DB(params["collection"])
	idx := mgo.Index{
		Key:        []string{"uuid", "-version"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     false,
	}
	db.C("superblocks").EnsureIndex(idx)
}

func NewMongoDBMetaProvider(params map[string]string) (*MongoDBMetaProvider, error) {
	provider := &MongoDBMetaProvider{}
	ses, err := mgo.Dial(params["server"])
	if err != nil {
		return nil, err
	}
	provider.ses = ses
	provider.db = ses.DB(params["collection"])
	return provider, nil
}

func (m *MongoDBMetaProvider) GetMeta(id string, version uint64) (*Meta, error) {
	meta := &Meta{}
	query := m.db.C("superblocks").Find(bson.M{"uuid": id, "version": version})
	if err := query.One(&meta); err != nil {
		if err == mgo.ErrNotFound {
			return nil, MetaNotFound
		}
		return nil, err
	}
	return meta, nil
}

func (m *MongoDBMetaProvider) GetLatestMeta(id string) (*Meta, error) {
	meta := &Meta{}
	query := m.db.C("superblocks").Find(bson.M{"uuid": id})
	if err := query.Sort("-version").One(&meta); err != nil {
		if err == mgo.ErrNotFound {
			return nil, MetaNotFound
		}
		return nil, err
	}
	return meta, nil
}

func (m *MongoDBMetaProvider) InsertMeta(meta *Meta) error {
	if err := m.db.C("superblocks").Insert(meta); err != nil {
		return err
	}
	return nil
}
