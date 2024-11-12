package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteReq struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteRes struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, req *RequestVoteReq, res *RequestVoteRes) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", req, res)
	return ok
}

func (rf *Raft) RequestVote(req *RequestVoteReq, res *RequestVoteRes) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// receive req from lower Term or already voted
	if req.Term < rf.currentTerm || (req.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != req.CandidateID) {
		DPrintf("Election Event: %s refuses to vote for Raft %d Term %d - Lower term or already voted\n", rf.getNodeInfo(), req.CandidateID, req.Term)
		res.Term = rf.currentTerm
		res.VoteGranted = false
		return
	}

	defer rf.persist()

	if req.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = req.Term
		rf.votedFor = -1
	}
	res.Term = rf.currentTerm

	if !rf.isLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		res.VoteGranted = false
		DPrintf("Election Event: %s refuses to vote for Raft %d Term %d - Log is not up to date\n", rf.getNodeInfo(), req.CandidateID, req.Term)
		return
	}

	rf.votedFor = req.CandidateID
	res.VoteGranted = true
	rf.resetElectionTimeout()
	DPrintf("Election Event: %s votes for Raft %d Term %d\n", rf.getNodeInfo(), req.CandidateID, req.Term)
}

func (rf *Raft) StartElection() {
	defer rf.persist()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me

	DPrintf("Election Event: %s starts election\n", rf.getNodeInfo())

	request := rf.makeRequestVoteReq()

	votes := 1
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				if rf.state != CANDIDATE {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				response := new(RequestVoteRes)
				if rf.sendRequestVote(peer, request, response) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.currentTerm == request.Term && rf.state == CANDIDATE {
						if response.VoteGranted {
							votes++
							if votes > len(rf.peers)/2 {
								rf.state = LEADER
								for i := range rf.nextIndex {
									rf.nextIndex[i] = rf.getLastLogIndex() + 1
								}
								rf.broadcastAppendEntries(true)
								rf.resetHeartbeat()
								rf.persist()
							}
						} else if response.Term > rf.currentTerm {
							rf.state = FOLLOWER
							rf.currentTerm = response.Term
							rf.votedFor = -1
							rf.resetElectionTimeout()
							rf.persist()
						}
					}
				}
			}(peer)
		}
	}
}
