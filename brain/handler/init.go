package handler

import (
	"github.com/iznauy/BTrDB/brain"
	"github.com/iznauy/BTrDB/brain/types"
)

func init() {
	brain.B.RegisterEventHandler(types.CreateBuffer, NewCreateBufferEventHandler())
	brain.B.RegisterEventHandler(types.AppendBuffer, NewAppendBufferEventHandler())
	brain.B.RegisterEventHandler(types.CommitBuffer, NewCommitBufferEventHandler())
}
