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

type InstallSnapshotReq struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	// Offset            int
	Data []byte
	// Done              bool
}

type InstallSnapshotRes struct {
	Term int
}

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesReq, res *AppendEntriesRes) bool {
	return rf.peers[server].Call("Raft.AppendEntries", req, res)
}

func (rf *Raft) sendInstallSnapshot(server int, req *InstallSnapshotReq, res *InstallSnapshotRes) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", req, res)
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, res *AppendEntriesRes) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term < rf.currentTerm {
		DPrintf("Replica Event: %s refuses entries from Raft %d Term %d - Smaller term\n", rf.getNodeInfo(), req.LeaderID, res.Term)
		res.Term = rf.currentTerm
		res.Success = false
		return
	}

	defer rf.persist()

	if req.Term > rf.currentTerm {
		rf.currentTerm = req.Term
		rf.votedFor = -1
	}

	rf.state = FOLLOWER
	rf.resetElectionTimeout()

	// todo: confliction or replication
	if rf.isAppendEntriesLogConflict(req, res) {
		return
	}

	DPrintf("Replica Event: %s accepts entries from Raft %d Term %d\n", rf.getNodeInfo(), req.LeaderID, req.Term)

	// Append any new entries not already in the log
	firstLogIndex := rf.getFirstLogIndex()
	rf.log = rf.log[0 : req.PrevLogIndex-firstLogIndex+1]
	rf.log = append(rf.log, req.Entries...)

	// Update commitIndex if leaderCommit is greater
	if req.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(req.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Signal()
	}

	res.Term = rf.currentTerm
	res.Success = true
}

func (rf *Raft) isAppendEntriesLogConflict(req *AppendEntriesReq, res *AppendEntriesRes) bool {
	lastLogIndex := rf.getLastLogIndex()
	if req.PrevLogIndex > lastLogIndex {
		res.XTerm = -1
		res.XIndex = lastLogIndex
		res.Success = false
		DPrintf("Replica Event: %s refuses entries from Raft %d Term %d - Shorter log\n", rf.getNodeInfo(), req.LeaderID, req.Term)
		return true
	}

	// Check if log contains an entry at PrevLogIndex with term matching PrevLogTerm
	firstLogIndex := rf.getFirstLogIndex()
	index := req.PrevLogIndex - firstLogIndex
	if rf.log[index].Term != req.PrevLogTerm {
		res.XTerm = rf.log[index].Term
		res.Success = false
		index = index - 1
		for index >= 0 && rf.log[index].Term == res.XTerm {
			index--
		}
		res.XIndex = index + 1
		DPrintf("Replica Event: %s refuses entries from Raft %d Term %d - Term confliction\n", rf.getNodeInfo(), req.LeaderID, req.Term)
		return true
	}

	return false
}

func (rf *Raft) InstallSnapshot(req *InstallSnapshotReq, res *InstallSnapshotRes) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	res.Term = rf.currentTerm

	if req.Term < rf.currentTerm {
		return
	}

	defer rf.persist()
	rf.votedFor = req.LeaderID
	rf.currentTerm = req.Term
	rf.state = FOLLOWER
	rf.resetElectionTimeout()

	if req.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      req.Data,
			SnapshotIndex: req.LastIncludedIndex,
			SnapshotTerm:  req.LastIncludedTerm,
		}
	}()

	if req.LastIncludedIndex > rf.getLastLogIndex() {
		rf.log = make([]LogEntry, 1)
	} else {
		indexInLog := req.LastIncludedIndex - rf.getFirstLogIndex()
		rf.log = rf.log[indexInLog:]
	}
	rf.log[0].Command = nil
	rf.log[0].Index = req.LastIncludedIndex
	rf.log[0].Term = req.LastIncludedTerm
	rf.lastApplied = req.LastIncludedIndex
	rf.commitIndex = req.LastIncludedIndex
}

// If it's a heartbeat, broadcast AppendEntries directly.
// Otherwise, wake up replicators.
func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	if isHeartbeat {
		DPrintf("Heartbeat Event: %s starts sending heartbeats\n", rf.getNodeInfo())
		for peer := range rf.peers {
			if peer != rf.me {
				go rf.replicateOneRound(peer)
			}
		}
	} else {
		// replica
		for peer := range rf.peers {
			if peer != rf.me {
				rf.replicatorCond[peer].Signal()
			}
		}
	}
}

// Process log replication in batches instead of handling it each time a log is appended
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplica(peer) {
			rf.replicatorCond[peer].Wait()
		}
		DPrintf("Replica Event: %s copy data to Raft %d\n", rf.getNodeInfo(), peer)
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplica(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == LEADER && rf.matchIndex[peer] < rf.getLastLogIndex()
}

// Send AppendEntries to peer server once
func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()
	if rf.state != LEADER { // during broadcast, leader may become follower
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[peer]-1 < rf.getFirstLogIndex() {
		req := rf.makeInstallSnapshotReq()
		rf.mu.Unlock()
		res := new(InstallSnapshotRes)

		if rf.sendInstallSnapshot(peer, req, res) {
			rf.mu.Lock()
			rf.handleInstallSnapshotRes(peer, req, res)
			rf.mu.Unlock()
		}
	} else {
		req := rf.makeAppendEntriesReq(peer)
		rf.mu.Unlock()
		res := new(AppendEntriesRes)

		if rf.sendAppendEntries(peer, req, res) {
			rf.mu.Lock()
			rf.handleAppendEntriesRes(peer, req, res)
			rf.mu.Unlock()
		}
	}
}

// If successful, update commitIndex.
// Otherwise, roll back nextIndex.
func (rf *Raft) handleAppendEntriesRes(peer int, req *AppendEntriesReq, res *AppendEntriesRes) {
	if res.Success {
		if len(req.Entries) != 0 {
			rf.matchIndex[peer] = req.PrevLogIndex + len(req.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.updateCommitIndex()
		}
		return
	}

	if res.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = res.Term
		rf.resetElectionTimeout()
		rf.persist()
		return
	}

	if res.XTerm != -1 {
		for i := len(rf.log) - 1; i >= 0; i-- {
			if rf.log[i].Term == res.XTerm {
				rf.nextIndex[peer] = rf.log[i].Index + 1
				break
			} else if rf.log[i].Term < res.XTerm {
				rf.nextIndex[peer] = res.XIndex // overwrite all entries with XTerm
				break
			}
		}
	} else {
		rf.nextIndex[peer] = res.XIndex + 1
	}
}

func (rf *Raft) updateCommitIndex() {
	lastLogEntry := rf.getLastLogEntry()
	if lastLogEntry.Term < rf.currentTerm { // Raft never commits log entries from previous terms by counting replicas
		return
	} else { // Append new entry of current term, need to commit all entries before this new entry
		nextCommitIndex := rf.commitIndex + 1
		if nextCommitIndex > lastLogEntry.Index {
			return
		}

		// Deal with server restarts
		firstLogIndex := rf.getFirstLogIndex()
		if rf.log[nextCommitIndex-firstLogIndex].Term != rf.currentTerm {
			for i := 0; i < len(rf.log); i++ {
				if rf.log[i].Term == rf.currentTerm {
					nextCommitIndex = rf.log[i].Index
					break
				}
			}
		}

		for nextCommitIndex <= lastLogEntry.Index {
			count := 1
			for peer := range rf.peers {
				if peer != rf.me && rf.matchIndex[peer] >= nextCommitIndex {
					count++
				}
			}
			if count >= (len(rf.peers)+1)/2 {
				rf.commitIndex = nextCommitIndex
				rf.applyCond.Signal()
			} else {
				break
			}
			nextCommitIndex++
		}
	}
}

func (rf *Raft) handleInstallSnapshotRes(peer int, req *InstallSnapshotReq, res *InstallSnapshotRes) {
	if req.Term != rf.currentTerm {
		return
	}
	if res.Term > rf.currentTerm {
		rf.currentTerm = res.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetElectionTimeout()
		rf.persist()
	} else {
		rf.matchIndex[peer] = req.LastIncludedIndex
		rf.nextIndex[peer] = req.LastIncludedIndex + 1
	}
}
