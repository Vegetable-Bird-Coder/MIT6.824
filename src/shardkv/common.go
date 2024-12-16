package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongOpType    = "ErrWrongOpType"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrExecuteTimeout = "ErrExecuteTimeout"
)

type Err string

const (
	OpGet    = "GET"
	OpPut    = "PUT"
	OpAppend = "APPEND"
)

type OpType string

type CommandRequest struct {
	Op        OpType
	Key       string
	Value     string
	ClientID  int64
	CommandID int64
}

type CommandResponse struct {
	Err   Err
	Value string
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
