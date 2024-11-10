package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) makeRequestVoteReq() *RequestVoteReq {
	lastLogEntry := rf.getLastLogEntry()
	return &RequestVoteReq{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
}

func (rf *Raft) makeAppendEntriesReq(peer int) *AppendEntriesReq {
	nextLogIndex := rf.nextIndex[peer]
	firstLogIndex := rf.getFirstLogIndex()
	return &AppendEntriesReq{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: nextLogIndex - 1,
		PrevLogTerm:  rf.log[nextLogIndex-1-firstLogIndex].Term,
		Entries:      rf.log[nextLogIndex-firstLogIndex:],
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	lastLogEntry := rf.getLastLogEntry()
	lastIndex := lastLogEntry.Index
	lastTerm := lastLogEntry.Term
	return lastTerm < lastLogTerm || (lastTerm == lastLogTerm && lastIndex <= lastLogIndex)
}

func (rf *Raft) getFirstLogEntry() *LogEntry {
	return &rf.log[0]
}

func (rf *Raft) getFirstLogIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) getLastLogEntry() *LogEntry {
	return &rf.log[len(rf.log)-1]
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) resetElectionTimeout() {
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(time.Duration(300+rand.Intn(200)) * time.Millisecond)
	} else {
		rf.electionTimer.Reset(time.Duration(300+rand.Intn(200)) * time.Millisecond)
	}

}

func (rf *Raft) resetHeartbeat() {
	if rf.heartbeatTimer == nil {
		rf.heartbeatTimer = time.NewTimer(time.Duration(150) * time.Millisecond)
	} else {
		rf.heartbeatTimer.Reset(time.Duration(150) * time.Millisecond)
	}
}
