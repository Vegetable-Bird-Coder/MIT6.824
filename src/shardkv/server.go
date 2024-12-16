package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const ExecuteTimeout = time.Duration(500) * time.Millisecond

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
	CommandId int64
	Response  *CommandResponse
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead          int32
	sc            *shardctrler.Clerk
	lastApplied   int
	stateMachine  KVStateMachine
	clientsLastOp map[int64]*OpContext
	responseChans map[int]chan *CommandResponse
	config        shardctrler.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.clientsLastOp = make(map[int64]*OpContext)
	kv.responseChans = make(map[int]chan *CommandResponse)

	kv.loadSnapshot(persister.ReadSnapshot())

	// Use something like this to talk to the shardctrler:
	kv.sc = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.config = kv.sc.Query(-1)

	go kv.cfgChecker()
	go kv.applier()

	return kv
}

func (kv *ShardKV) Command(req *CommandRequest, res *CommandResponse) {
	kv.mu.Lock()
	if req.Op != OpGet && kv.isDuplicatedOp(req.ClientID, req.CommandID) {
		response := kv.clientsLastOp[req.ClientID].Response
		res.Value = response.Value
		res.Err = response.Err
		DPrintf("Node %d receives duplicate request %s\n", kv.me, req.String())
		kv.mu.Unlock()
		return
	}
	shard := key2shard(req.Key)
	gid := kv.config.Shards[shard]
	kv.mu.Unlock()

	if gid != kv.gid {
		res.Err = ErrWrongLeader
		return
	}

	op := Op{Type: req.Op, Key: req.Key, Value: req.Value, ClientID: req.ClientID, CommandID: req.CommandID}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}
	DPrintf("Node %d receives request %s in index %d\n", kv.me, req.String(), index)

	kv.mu.Lock()
	responseChan := kv.getResponseChan(index)
	kv.mu.Unlock()

	select {
	case response := <-responseChan:
		res.Value = response.Value
		res.Err = response.Err
	case <-time.After(ExecuteTimeout):
		res.Err = ErrExecuteTimeout
	}
	DPrintf("Node %d response %s for request %s in index %d\n", kv.me, res.String(), req.String(), index)

	go func() {
		kv.mu.Lock()
		close(kv.responseChans[index])
		delete(kv.responseChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		message := <-kv.applyCh
		if message.CommandValid {
			kv.mu.Lock()
			DPrintf("Node %d applies command %s\n", kv.me, applyMsgToString(&message))
			// skip outdated message
			if message.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				DPrintf("Node %d gets out-date message %v\n", kv.me, message)
				continue
			}
			kv.lastApplied = message.CommandIndex

			op := message.Command.(Op)

			var res *CommandResponse
			if op.Type != OpGet && kv.isDuplicatedOp(op.ClientID, op.CommandID) { // one command may timeout and get the same command next time
				res = kv.clientsLastOp[op.ClientID].Response
			} else {
				value, err := kv.updateStateMachine(&op)
				res = &CommandResponse{Value: value, Err: err}
				if op.Type != OpGet {
					kv.clientsLastOp[op.ClientID] = &OpContext{CommandId: op.CommandID, Response: res}
				}
			}

			if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == message.CommandTerm {
				responseChan := kv.getResponseChan(message.CommandIndex)
				responseChan <- res
			}

			kv.Snapshot(message.CommandIndex)

			kv.mu.Unlock()
		} else if message.SnapshotValid {
			kv.mu.Lock()
			DPrintf("Node %d applies snapshot %s\n", kv.me, applyMsgToString(&message))
			kv.loadSnapshot(message.Snapshot)
			kv.lastApplied = message.SnapshotIndex
			kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("Unexpected Message %v", message))
		}
	}
}

func (kv *ShardKV) cfgChecker() {
	for !kv.killed() {
		cfg := kv.sc.Query(-1)
		kv.mu.Lock()
		kv.config = cfg
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) isDuplicatedOp(clientID, commandID int64) bool {
	if opContext, ok := kv.clientsLastOp[clientID]; ok {
		if opContext.CommandId == commandID {
			return true
		}
	}
	return false
}

func (kv *ShardKV) getResponseChan(index int) chan *CommandResponse {
	if _, ok := kv.responseChans[index]; !ok {
		kv.responseChans[index] = make(chan *CommandResponse, 1)
	}
	return kv.responseChans[index]
}

func (kv *ShardKV) Snapshot(index int) {
	if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate {
		buffer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(buffer)
		encoder.Encode(kv.stateMachine)
		encoder.Encode(kv.clientsLastOp)
		snapshot := buffer.Bytes()
		kv.rf.Snapshot(index, snapshot)
	}
}

func (kv *ShardKV) loadSnapshot(data []byte) {
	kv.stateMachine = newMemoryKV()
	kv.clientsLastOp = make(map[int64]*OpContext)

	if data == nil || len(data) < 1 {
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	if decoder.Decode(kv.stateMachine) != nil || decoder.Decode(&kv.clientsLastOp) != nil {
		DPrintf("read persist error\n")
	} else {
		DPrintf("Reboot Event: reload snapshot\n")
	}
}
