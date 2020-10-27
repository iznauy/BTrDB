package magic

import "sync"

// TODO: 到底收集哪些系统信息，等策略固定下来再说，这边就先置空

type OsStatus struct {
	cpuInfo  *CPUInfo
	memInfo  *MemoryInfo
	diskInfo *DiskInfo
	netInfo  *NetInfo
}

type MemoryInfo struct {
	mu sync.RWMutex
}

type CPUInfo struct {
	mu sync.RWMutex
}

type DiskInfo struct {
	mu sync.RWMutex
}

type NetInfo struct {
	mu sync.RWMutex
}

func newOsStatus() *OsStatus {
	return &OsStatus{
		cpuInfo:  newCPUInfo(),
		memInfo:  newMemoryInfo(),
		diskInfo: newDiskInfo(),
		netInfo:  newNetInfo(),
	}
}

func newMemoryInfo() *MemoryInfo {
	return &MemoryInfo{}
}

func newCPUInfo() *CPUInfo {
	return &CPUInfo{}
}

func newDiskInfo() *DiskInfo {
	return &DiskInfo{}
}

func newNetInfo() *NetInfo {
	return &NetInfo{}
}

func (os *OsStatus) gatherOsStatus() {
	os.cpuInfo.gatherCPUInfo()
	os.memInfo.gatherMemoryInfo()
	os.diskInfo.gatherDiskInfo()
	os.netInfo.gatherNetInfo()
}

func (cpu *CPUInfo) gatherCPUInfo() {

}

func (mem *MemoryInfo) gatherMemoryInfo() {

}

func (disk *DiskInfo) gatherDiskInfo() {

}

func (net *NetInfo) gatherNetInfo() {

}
