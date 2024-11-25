package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ExecuteTimeout = time.Duration(3) * time.Second

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      OpType
	Key       string
	Value     string
	ClientID  int64
	CommandID int64
}

type OpContext struct {
	commandId int64
	response  *CommandResponse
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied   int
	stateMachine  KVStateMachine
	clientsLastOp map[int64]*OpContext
	responseChans map[int]chan *CommandResponse
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.stateMachine = newMemoryKV()
	kv.clientsLastOp = make(map[int64]*OpContext)
	kv.responseChans = make(map[int]chan *CommandResponse)

	go kv.applier()

	return kv
}

func (kv *KVServer) Command(req *CommandRequest, res *CommandResponse) {
	kv.mu.Lock()
	if req.Op != OpGet && kv.isDuplicatedOp(req.ClientID, req.CommandID) {
		response := kv.clientsLastOp[req.ClientID].response
		res.Value = response.Value
		res.Error = response.Error
		DPrintf("Node %d receives duplicate request %s\n", kv.me, req.String())
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{Type: req.Op, Key: req.Key, Value: req.Value, ClientID: req.ClientID, CommandID: req.CommandID}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Error = ErrWrongLeader
		return
	}
	DPrintf("Node %d receives request %s in index %d\n", kv.me, req.String(), index)

	kv.mu.Lock()
	responseChan := kv.getResponseChan(index)
	kv.mu.Unlock()

	select {
	case response := <-responseChan:
		res.Value = response.Value
		res.Error = response.Error
	case <-time.After(ExecuteTimeout):
		res.Error = ErrExecuteTimeout
	}
	DPrintf("Node %d response %s for request %s in index %d\n", kv.me, res.String(), req.String(), index)

	go func() {
		kv.mu.Lock()
		close(kv.responseChans[index])
		delete(kv.responseChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		message := <-kv.applyCh
		DPrintf("Node %d start to apply message %v\n", kv.me, message)
		if message.CommandValid {
			kv.mu.Lock()
			// skip outdated message
			if message.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				DPrintf("Node %d get out-date message %v\n", kv.me, message)
				continue
			}
			kv.lastApplied = message.CommandIndex

			op := message.Command.(Op)

			var res *CommandResponse
			if op.Type != OpGet && kv.isDuplicatedOp(op.ClientID, op.CommandID) { // one command may timeout and get the same command next time
				res = kv.clientsLastOp[op.ClientID].response
			} else {
				value, err := kv.updateStateMachine(&op)
				res = &CommandResponse{Value: value, Error: err}
				if op.Type != OpGet {
					kv.clientsLastOp[op.ClientID] = &OpContext{commandId: op.CommandID, response: res}
				}
			}

			// DPrintf("Node %d applying message %v\n", kv.me, message)
			if _, isLeader := kv.rf.GetState(); isLeader {
				responseChan := kv.getResponseChan(message.CommandIndex)
				responseChan <- res
			}
			// DPrintf("Node %d finish to apply message %v\n", kv.me, message)
			kv.mu.Unlock()
		} else if message.SnapshotValid {

		} else {
			panic(fmt.Sprintf("Unexpected Message %v", message))
		}
	}
}

func (kv *KVServer) isDuplicatedOp(clientID, commandID int64) bool {
	if opContext, ok := kv.clientsLastOp[clientID]; ok {
		if opContext.commandId == commandID {
			return true
		}
	}
	return false
}

func (kv *KVServer) getResponseChan(index int) chan *CommandResponse {
	if _, ok := kv.responseChans[index]; !ok {
		kv.responseChans[index] = make(chan *CommandResponse, 1)
	}
	return kv.responseChans[index]
}
