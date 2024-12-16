package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientID  int64
	commandID int64
	leaderID  int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientID = nrand()
	return ck
}

func (ck *Clerk) Get(key string) string {
	commandReq := &CommandRequest{
		Op:  OpGet,
		Key: key,
	}
	return ck.Command(commandReq)
}

func (ck *Clerk) Put(key string, value string) {
	commandReq := &CommandRequest{
		Op:    OpPut,
		Key:   key,
		Value: value,
	}
	ck.Command(commandReq)
}
func (ck *Clerk) Append(key string, value string) {
	commandReq := &CommandRequest{
		Op:    OpAppend,
		Key:   key,
		Value: value,
	}
	ck.Command(commandReq)
}

func (ck *Clerk) Command(commandReq *CommandRequest) string {
	commandReq.ClientID = ck.clientID
	commandReq.CommandID = ck.commandID

	for {
		shard := key2shard(commandReq.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for {
				commandRes := new(CommandResponse)
				srv := ck.make_end(servers[ck.leaderID])
				if !srv.Call("ShardKV.Command", commandReq, commandRes) || commandRes.Err == ErrWrongLeader || commandRes.Err == ErrExecuteTimeout {
					ck.leaderID = (ck.leaderID + 1) % len(servers)
					continue
				}
				if commandRes.Err == ErrWrongGroup {
					break
				}
				ck.commandID++
				return commandRes.Value
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}
