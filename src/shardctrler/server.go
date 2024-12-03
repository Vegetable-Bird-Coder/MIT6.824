package shardctrler

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const ExecuteTimeout = time.Duration(500) * time.Millisecond

type OpContext struct {
	CommandId int64
	Response  *CommandResponseAux
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastApplied        int
	clientsLastOp      map[int64]*OpContext
	responseChans      map[int]chan *CommandResponseAux
	configStateMachine ConfigStateMachine
}

type Op struct {
	// Your data here.
	Servers   map[int][]string // new GID -> servers mappings, for Join
	GIDs      []int            // for Leave
	Shard     int              // for Move
	GID       int              // for Move
	Num       int              // desired config number, for Query
	Op        OpType
	ClientID  int64
	CommandID int64
}

func (sc *ShardCtrler) Command(req *CommandRequest, res *CommandResponse) {
	sc.mu.Lock()
	if req.Op != OpQuery && sc.isDuplicatedOp(req.ClientID, req.CommandID) {
		response := sc.clientsLastOp[req.ClientID].Response
		res.Config = *response.Config
		res.Err = response.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		Servers:   req.Servers,
		GIDs:      req.GIDs,
		Shard:     req.Shard,
		GID:       req.GID,
		Num:       req.Num,
		Op:        req.Op,
		ClientID:  req.ClientID,
		CommandID: req.CommandID,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}
	DPrintf("Request Event: Node %d receives request %s in index %d\n", sc.me, req.String(), index)

	sc.mu.Lock()
	responseChan := sc.getResponseChan(index)
	sc.mu.Unlock()

	select {
	case response := <-responseChan:
		res.Config = *response.Config
		res.Err = response.Err
		DPrintf("Response Event: Node %d response %s for request %s in index %d\n", sc.me, response.String(), req.String(), index)
	case <-time.After(ExecuteTimeout):
		res.Err = ErrExecuteTimeout
		DPrintf("Response Event: Node %d response %s for request %s in index %d\n", sc.me, res.Err, req.String(), index)
	}

	go func() {
		sc.mu.Lock()
		close(sc.responseChans[index])
		delete(sc.responseChans, index)
		sc.mu.Unlock()
	}()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientsLastOp = make(map[int64]*OpContext)
	sc.responseChans = make(map[int]chan *CommandResponseAux)
	sc.configStateMachine = newMemoryConfig()

	go sc.applier()
	return sc
}

func (sc *ShardCtrler) applier() {
	for {
		message := <-sc.applyCh
		if message.CommandValid {
			sc.mu.Lock()
			sc.lastApplied = message.CommandIndex

			op := message.Command.(Op)
			res := new(CommandResponseAux)
			if op.Op != OpQuery && sc.isDuplicatedOp(op.ClientID, op.CommandID) {
				res = sc.clientsLastOp[op.ClientID].Response
				DPrintf("Apply Event: Node %d ignores duplicate command %s at index %d\n", sc.me, applyMsgToString(&message), message.CommandIndex)
			} else {
				switch op.Op {
				case OpJoin:
					res.Err, res.Config = sc.configStateMachine.Join(op.Servers)
				case OpLeave:
					res.Err, res.Config = sc.configStateMachine.Leave(op.GIDs)
				case OpMove:
					res.Err, res.Config = sc.configStateMachine.Move(op.Shard, op.GID)
				case OpQuery:
					res.Err, res.Config = sc.configStateMachine.Query(op.Num)
				default:
					res.Err = ErrUnexpectOpType
				}
				if op.Op != OpQuery {
					sc.clientsLastOp[op.ClientID] = &OpContext{op.CommandID, res}
				}
				DPrintf("Apply Event: Node %d applies command %s at index %d ending with status %s\n", sc.me, applyMsgToString(&message), message.CommandIndex, res.String())
			}

			if currentTerm, isLeader := sc.rf.GetState(); isLeader && currentTerm == message.CommandTerm {
				responseChan := sc.getResponseChan(message.CommandIndex)
				responseChan <- res
			}

			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) isDuplicatedOp(clientID, commandID int64) bool {
	if opContext, ok := sc.clientsLastOp[clientID]; ok {
		if opContext.CommandId == commandID {
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) getResponseChan(index int) chan *CommandResponseAux {
	if _, ok := sc.responseChans[index]; !ok {
		sc.responseChans[index] = make(chan *CommandResponseAux, 1)
	}
	return sc.responseChans[index]
}
