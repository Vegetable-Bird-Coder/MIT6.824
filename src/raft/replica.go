package raft

type AppendEntriesReq struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesRes struct {
	Term    int
	Success bool
	XTerm   int // Conflict term
	XIndex  int // First index of the conflict term  or index of last entry
}

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesReq, res *AppendEntriesRes) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, res)
	return ok
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, res *AppendEntriesRes) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term < rf.currentTerm {
		res.Term = rf.currentTerm
		res.Success = false
		DPrintf("Raft %d term %d refuses entries from Raft %d term %d: smaller term\n", rf.me, rf.currentTerm, req.LeaderID, res.Term)
		return
	}

	if req.Term > rf.currentTerm {
		rf.currentTerm = req.Term
		rf.votedFor = -1
	}

	rf.state = FOLLOWER
	rf.resetElectionTimeout()

	// todo: confliction or replication

	res.Term = rf.currentTerm
	res.Success = true
	DPrintf("Raft %d term %d accepts entries from Raft %d term %d\n", rf.me, rf.currentTerm, req.LeaderID, req.Term)
}

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	if isHeartbeat {
		DPrintf("Raft %d term %d starts sending heartbeats\n", rf.me, rf.currentTerm)
		for peer := range rf.peers {
			if peer != rf.me {
				go rf.replicateOneRound(peer)
			}
		}
	} else {
		// todo: replication
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()
	request := rf.makeAppendEntriesReq(peer)
	rf.mu.Unlock()

	response := new(AppendEntriesRes)
	if rf.sendAppendEntries(peer, request, response) {
		rf.mu.Lock()
		rf.handleAppendEntriesResponse(peer, request, response)
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, req *AppendEntriesReq, res *AppendEntriesRes) {
	// todo
	// for 3A, we don't need to implement this
}
