package variable

type BufferAllocationPolicy int

var (
	PreAllocation BufferAllocationPolicy = 1
	LinkedList    BufferAllocationPolicy = 2
)

func GetBufferAllocationPolicy(env map[string]interface{}) BufferAllocationPolicy {
	return PreAllocation
}
