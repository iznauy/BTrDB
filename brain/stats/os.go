package stats

import (
	"github.com/akhenakh/statgo"
	"sync"
	"time"
)

type OsStats struct {
	stats *statgo.Stat

	CPU   *statgo.CPUStats
	cpuMu sync.RWMutex

	DiskIO []*statgo.DiskIOStats
	diskMu sync.RWMutex

	NetIO []*statgo.NetIOStats
	netMu sync.RWMutex

	Mem   *statgo.MemStats
	memMu sync.RWMutex
}

func NewOsStats() *OsStats {
	os := &OsStats{
		stats: statgo.NewStat(),
	}
	os.CPU = os.stats.CPUStats()
	os.DiskIO = os.stats.DiskIOStats()
	os.NetIO = os.stats.NetIOStats()
	os.Mem = os.stats.MemStats()
	go os.vary()
	return os
}

func (os *OsStats) vary() {
	for {
		time.Sleep(30)

		os.cpuMu.Lock()
		os.CPU = os.stats.CPUStats()
		os.cpuMu.Unlock()

		os.diskMu.Lock()
		os.DiskIO = os.stats.DiskIOStats()
		os.diskMu.Unlock()

		os.netMu.Lock()
		os.NetIO = os.stats.NetIOStats()
		os.netMu.Unlock()

		os.memMu.Lock()
		os.Mem = os.stats.MemStats()
		os.memMu.Unlock()
	}
}
