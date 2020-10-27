package magic

import (
	"errors"
	"github.com/iznauy/BTrDB/conf"
	"time"
)

var Engine *Magic

func init() {
}

// 绝大多数的策略都空置了，相当于没有策略

type Magic struct {
	config *conf.Config
	app    *ApplicationStatistics
	os     *OsStatus
}

// 守护线程，定期从周围环境中收集用得到的统计信息
func (e *Magic) daemon() {
	for {
		time.Sleep(10 * time.Second)
		e.os.gatherOsStatus()
	}
}

func InitMagicEngine(config *conf.Config) {
	m := &Magic{
		config: config,
		app:    newApplicationStatistics(),
		os:     newOsStatus(),
	}
	go m.daemon()
}

func (e *Magic) GetKAndFForNewTimeSeries(uuid [16]byte) (K uint16, F uint32, err error) {
	buffer := e.app.buffer
	entry, mu := buffer.getBufferStatisticsEntry(uuid)
	mu.Lock()
	if !entry.newTree {
		return 0, 0, errors.New("not new timeseries")
	}
	return 64, 1024, nil
}

func (e *Magic) GetCacheMaxSize() uint64 {
	e.app.cache.mu.RLock()
	defer e.app.cache.mu.RUnlock()
	// 没有任何策略，直接让 cache size 保持不变
	return e.app.cache.cacheSize
}

func (e *Magic) GetReadWriteLimiter() (int64, int64) {
	return e.app.dataTraffic.readLimiter, e.app.dataTraffic.writeLimiter
}

func (e *Magic) GetCommitInterval() uint64 {
	return 0
}

func (e *Magic) GetBufferMaxSize() uint64 {
	return 0
}
