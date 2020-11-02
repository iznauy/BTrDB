package stats

type ApplicationStats struct {
	WorkLoad *WorkLoadStats
}

type WorkLoadStats struct {
	TotalReadBytes uint64
	TotalWriteBytes uint64

	RecentReadBytes uint64
	RecentWriteBytes uint64

}


func NewApplicationStats() *ApplicationStats {
	return &ApplicationStats{}
}

func NewWorkLoadStats() *WorkLoadStats {
	return &WorkLoadStats{
		
	}
}