package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID  int64
	leaderID  int
	commandID int64
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
	// You'll have to add code here.
	ck.clientID = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	return ck.Command(&CommandRequest{Key: key, Op: OpGet})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpPut})
}

func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpAppend})
}

func (ck *Clerk) Command(req *CommandRequest) string {
	req.ClientID = ck.clientID
	req.CommandID = ck.commandID
	// DPrintf("Clerk %d dealing with Op %d key %s value %s with commandID %d\n", req.ClientID, req.Op, req.Key, req.Value, req.CommandID)
	for {
		res := new(CommandResponse)
		if !ck.servers[ck.leaderID].Call("KVServer.Command", req, res) || res.Error == ErrWrongLeader || res.Error == ErrExecuteTimeout {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		// DPrintf("Clerk %d receives response value %s commandID %d\n", req.ClientID, res.Value, req.CommandID)
		ck.commandID++
		return res.Value
	}
}
