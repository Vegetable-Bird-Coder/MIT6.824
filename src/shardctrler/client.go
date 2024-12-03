package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientID  int64
	commandID int64
	leaderID  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientID = nrand()
	ck.commandID = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	commandReq := new(CommandRequest)
	commandReq.Num = num
	commandReq.Op = OpQuery
	return ck.Command(commandReq).Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	commandReq := new(CommandRequest)
	commandReq.Servers = servers
	commandReq.Op = OpJoin
	ck.Command(commandReq)
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	commandReq := new(CommandRequest)
	commandReq.GIDs = gids
	commandReq.Op = OpLeave
	ck.Command(commandReq)
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	commandReq := new(CommandRequest)
	commandReq.Shard = shard
	commandReq.GID = gid
	commandReq.Op = OpMove
	ck.Command(commandReq)
}

func (ck *Clerk) Command(commandReq *CommandRequest) *CommandResponse {
	commandReq.ClientID = ck.clientID
	commandReq.CommandID = ck.commandID
	for {
		commandRes := new(CommandResponse)
		if !ck.servers[ck.leaderID].Call("ShardCtrler.Command", commandReq, commandRes) || commandRes.Err == ErrWrongLeader || commandRes.Err == ErrExecuteTimeout {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.commandID++
		return commandRes
	}
}
