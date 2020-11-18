package conf

const (
	StateSequenceMaxLength = 100
	Alpha                  = 0.5
	Beta                   = 0.999
	SampleCount            = 50
	DecisionSetCount       = 4
	DefaultBufferSize      = 10000 // 在系统里一个时间序列都没有的情况下，
	DefaultCommitInterval  = 10000
)
