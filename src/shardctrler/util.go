package shardctrler

import (
	"fmt"
	"log"
	"sort"

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

func (commandReq *CommandRequest) String() string {
	str := ""
	switch commandReq.Op {
	case OpJoin:
		var gids []int
		for gid := range commandReq.Servers {
			gids = append(gids, gid)
		}
		sort.Ints(gids)
		for _, gid := range gids {
			servers := commandReq.Servers[gid]
			str += fmt.Sprintf("{%d:", gid)
			for _, server := range servers {
				str += fmt.Sprintf(" %s", server)
			}
			str += "}"
		}
	case OpLeave:
		str = "{"
		for _, gid := range commandReq.GIDs {
			str += fmt.Sprintf("%d ", gid)
		}
		str += "}"
	case OpMove:
		str = fmt.Sprintf("{Shard %d Gid %d}", commandReq.Shard, commandReq.GID)
	case OpQuery:
		str = fmt.Sprintf("{Num %d}", commandReq.Num)
	default:
		str = "Unexpected Op Type"
	}
	return fmt.Sprintf("(Client %d Command %d Op %s Servers %s)", commandReq.ClientID, commandReq.CommandID, commandReq.Op, str)
}

func (commandRes *CommandResponseAux) String() string {
	if commandRes.Err == OK && commandRes.Config != nil {
		str := fmt.Sprintf("{Num: %d, Shards: ", commandRes.Config.Num)
		gid2shards := makeGid2Shards(commandRes.Config)
		var gids []int
		for gid := range gid2shards {
			gids = append(gids, gid)
		}
		sort.Ints(gids)
		for _, gid := range gids {
			str += fmt.Sprintf("(%d:", gid)
			for _, shard := range gid2shards[gid] {
				str += fmt.Sprintf(" %d", shard)
			}
			str += ")"
		}

		str += ", Groups: "
		gids = make([]int, 0)
		for gid := range commandRes.Config.Groups {
			gids = append(gids, gid)
		}
		sort.Ints(gids)
		for _, gid := range gids {
			str += fmt.Sprintf("(%d: ", gid)
			for _, server := range commandRes.Config.Groups[gid] {
				str += fmt.Sprintf("%s ", server)
			}
			str += ")"
		}
		str += "}"
		return fmt.Sprintf("%s %s", commandRes.Err, str)
	}
	return string(commandRes.Err)
}

func deepCopy(original map[int][]string) map[int][]string {
	aux := make(map[int][]string)
	for key, value := range original {
		aux[key] = make([]string, len(value))
		copy(aux[key], value)
	}
	return aux
}

func makeGid2Shards(cf *Config) map[int][]int {
	gid2shards := make(map[int][]int)
	gid2shards[0] = make([]int, 0)
	for gid := range cf.Groups {
		gid2shards[gid] = make([]int, 0)
	}
	for shard, gid := range cf.Shards {
		gid2shards[gid] = append(gid2shards[gid], shard)
	}
	return gid2shards
}

func getGidWithMinShards(gid2shards map[int][]int) int {
	var gids []int
	for gid := range gid2shards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	res, shardNum := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(gid2shards[gid]) < shardNum {
			res = gid
			shardNum = len(gid2shards[gid])
		}
	}
	return res
}

func getGidWithMaxShards(gid2shards map[int][]int) int {
	if len(gid2shards[0]) > 0 {
		return 0
	}

	var gids []int
	for gid := range gid2shards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	res, shardNum := -1, -1
	for _, gid := range gids {
		if len(gid2shards[gid]) > shardNum {
			res = gid
			shardNum = len(gid2shards[gid])
		}
	}
	return res
}

func applyMsgToString(msg *raft.ApplyMsg) string {
	if msg.CommandValid {
		op := msg.Command.(Op)
		commandReq := &CommandRequest{
			Servers:   op.Servers,
			GIDs:      op.GIDs,
			Shard:     op.Shard,
			GID:       op.GID,
			Num:       op.Num,
			Op:        op.Op,
			ClientID:  op.ClientID,
			CommandID: op.CommandID,
		}
		return commandReq.String()
	} else {
		return fmt.Sprintf("(Content %v Index %d Term %d)", msg.Snapshot, msg.SnapshotIndex, msg.SnapshotTerm)
	}
}
