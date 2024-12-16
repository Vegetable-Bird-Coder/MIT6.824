package shardkv

import (
	"fmt"
	"log"

	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Stringer interface {
	String() string
}

func (commandRequest *CommandRequest) String() string {
	return fmt.Sprintf("(ClientID %d CommandID %d Op %s Key %s Value %s)", commandRequest.ClientID, commandRequest.CommandID, commandRequest.Op, commandRequest.Key, commandRequest.Value)
}

func (commandResponse *CommandResponse) String() string {
	return fmt.Sprintf("(Value %s Err %s)", commandResponse.Value, commandResponse.Err)
}

func applyMsgToString(msg *raft.ApplyMsg) string {
	if msg.CommandValid {
		op := msg.Command.(Op)
		return fmt.Sprintf("(ClientID %d CommandID %d Op %s Key %s Value %s)", op.ClientID, op.CommandID, op.Type, op.Key, op.Value)
	} else {
		return fmt.Sprintf("(Content %v Index %d Term %d)", msg.Snapshot, msg.SnapshotIndex, msg.SnapshotTerm)
	}
}
