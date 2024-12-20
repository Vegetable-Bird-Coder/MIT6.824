package shardctrler

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK                = "OK"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrExecuteTimeout = "ErrExecuteTimeout"
	ErrUnexpectOpType = "ErrUnexpectOpType"
)

type Err string

const (
	OpJoin  = "JOIN"
	OpLeave = "LEAVE"
	OpMove  = "MOVE"
	OpQuery = "QUERY"
)

type OpType string

type CommandRequest struct {
	Servers   map[int][]string // new GID -> servers mappings, for Join
	GIDs      []int            // for Leave
	Shard     int              // for Move
	GID       int              // for Move
	Num       int              // desired config number, for Query
	Op        OpType
	ClientID  int64
	CommandID int64
}

type CommandResponse struct {
	Err    Err
	Config Config
}

type CommandResponseAux struct {
	Err    Err
	Config *Config
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
