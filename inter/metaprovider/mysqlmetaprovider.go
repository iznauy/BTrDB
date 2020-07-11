package metaprovider

type MySQLMetaProvider struct {
}

func CreateMySQLMetaDatabase(params map[string]string) {
}

func NewMySQLMetaProvider(params map[string]string) (*MongoDBMetaProvider, error) {
	return nil, nil
}

func (m *MySQLMetaProvider) GetMeta(id string, version uint64) (*Meta, error) {
	return nil, nil
}

func (m *MySQLMetaProvider) GetLatestMeta(id string) (*Meta, error) {
	return nil, nil
}

func (m *MySQLMetaProvider) InsertMeta(meta *Meta) error {
	return nil
}
