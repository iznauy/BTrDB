package stats

import "sync"

type OsStats struct {
	Memory *MemoryStats
	Cpu    *CpuStats
	Disk   *DiskStats
	Net    *NetStats
}

type MemoryStats struct {
	mu sync.RWMutex
}

type CpuStats struct {
	mu sync.RWMutex
}

type DiskStats struct {
	mu sync.RWMutex
}

type NetStats struct {
	mu sync.RWMutex
}

func NewOsStats() *OsStats {
	return &OsStats{
		Memory: NewMemoryStats(),
		Cpu:    NewCpuStats(),
		Disk:   NewDiskStats(),
		Net:    NewNetStats(),
	}
}

func NewMemoryStats() *MemoryStats {
	return &MemoryStats{}
}

func NewCpuStats() *CpuStats {
	return &CpuStats{}
}

func NewDiskStats() *DiskStats {
	return &DiskStats{}
}

func NewNetStats() *NetStats {
	return &NetStats{}
}

func (os *OsStats) gatherOsStats() {
	os.Memory.gatherMemoryStats()
	os.Cpu.gatherCpuStats()
	os.Net.gatherNetStats()
	os.Disk.gatherDiskStats()
}

func (mem *MemoryStats) gatherMemoryStats() {

}

func (cpu *CpuStats) gatherCpuStats() {

}

func (disk *DiskStats) gatherDiskStats() {

}

func (net *NetStats) gatherNetStats() {

}
