package kvraft

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrWrongOpType    = "ErrWrongOpType"
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
	Key       string
	Value     string
	Op        OpType
	ClientID  int64
	CommandID int64
}

type CommandResponse struct {
	Value string
	Error Err
}
