package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Leader struct {
	nextIndex  []int
	matchIndex []int
	leader_per Persistent
	leader_vol Volatile
}

type Persistent struct {
	currentTerm int
	votedFor    int
	log         []string
}

type Volatile struct {
	commitIndex int
	lastApplied int
}

type Role int

const (
	follower Role = iota
	candidate
	leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int

	role Role

	lastResetElectionTime time.Time
	electionTimeout       time.Duration

	// persistent state
	currentTerm int
	vodtedFor   int
	log         []LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// leader state
	nextIndex  []int
	matchIndex []int

	applyChan chan raftapi.ApplyMsg
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

func (rf *Raft) persist() {
	// 3C
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.EN
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// 3C
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 3D
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) getTimeout() time.Duration {
	n := rand.Intn(300)
	return time.Duration(n)*time.Millisecond + 300*time.Millisecond
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.lastResetElectionTime = time.Now()
	rf.electionTimeout = rf.getTimeout()
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.vodtedFor = -1
	}
	rf.role = follower
	rf.resetElectionTimerLocked()
}

func (rf *Raft) lastLogIndexTermLocked() (int, int) {
	lastIndex := len(rf.log) - 1
	return lastIndex, rf.log[lastIndex].Term
}

func (rf *Raft) collectApplyMsgsLocked() []raftapi.ApplyMsg {
	if rf.lastApplied >= rf.commitIndex {
		return nil
	}

	msgs := make([]raftapi.ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msgs = append(msgs, raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		})
	}
	rf.lastApplied = rf.commitIndex
	return msgs
}

func (rf *Raft) sendApplyMsgs(msgs []raftapi.ApplyMsg) {
	for _, msg := range msgs {
		rf.applyChan <- msg
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) updateCommitIndexLocked() {
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		if rf.log[N].Term != rf.currentTerm {
			continue
		}
		cnt := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= N {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = N
			return
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	reply.Term = rf.currentTerm

	myLastIndex, myLastTerm := rf.lastLogIndexTermLocked()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if upToDate && (rf.vodtedFor == -1 || rf.vodtedFor == args.CandidateId) {
		rf.vodtedFor = args.CandidateId
		rf.resetElectionTimerLocked()
		reply.VoteGranted = true
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) AppendEntriesVote(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	} else {
		rf.role = follower
		rf.resetElectionTimerLocked()
	}
	reply.Term = rf.currentTerm

	if args.PrevLogIndex >= len(rf.log) {
		rf.mu.Unlock()
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.mu.Unlock()
		return
	}

	for i, entry := range args.Entries {
		pos := args.PrevLogIndex + 1 + i
		if pos < len(rf.log) {
			if rf.log[pos].Term != entry.Term {
				rf.log = rf.log[:pos]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, len(rf.log)-1)
	}
	msgs := rf.collectApplyMsgsLocked()
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.mu.Unlock()

	rf.sendApplyMsgs(msgs)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntriesVote", args, reply)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.role != leader {
		term := rf.currentTerm
		rf.mu.Unlock()
		return -1, term, false
	}

	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})
	index := len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.mu.Unlock()

	go rf.broadcastHeartbeat()
	return index, term, true
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.role = candidate
	rf.currentTerm++
	rf.vodtedFor = rf.me
	termStarted := rf.currentTerm
	rf.resetElectionTimerLocked()
	lastLogIndex, lastLogTerm := rf.lastLogIndexTermLocked()
	votesReceived := 1
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := RequestVoteArgs{
				Term:         termStarted,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}

			needBroadcast := false

			rf.mu.Lock()
			if rf.role != candidate || rf.currentTerm != termStarted {
				rf.mu.Unlock()
				return
			}

			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				votesReceived++
				if votesReceived > len(rf.peers)/2 && rf.role == candidate {
					rf.role = leader
					lastIdx := len(rf.log) - 1
					for j := range rf.peers {
						rf.nextIndex[j] = len(rf.log)
						rf.matchIndex[j] = 0
					}
					rf.matchIndex[rf.me] = lastIdx
					rf.nextIndex[rf.me] = lastIdx + 1
					needBroadcast = true
				}
			}
			rf.mu.Unlock()

			if needBroadcast {
				go rf.broadcastHeartbeat()
			}
		}(i)
	}
}

func (rf *Raft) applyCommittedEntries() {
	rf.mu.Lock()
	msgs := rf.collectApplyMsgsLocked()
	rf.mu.Unlock()
	rf.sendApplyMsgs(msgs)
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	if rf.role != leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			for {
				rf.mu.Lock()
				if rf.role != leader {
					rf.mu.Unlock()
					return
				}

				nextIdx := rf.nextIndex[server]
				prevLogIndex := nextIdx - 1
				prevLogTerm := rf.log[prevLogIndex].Term
				entries := make([]LogEntry, len(rf.log[nextIdx:]))
				copy(entries, rf.log[nextIdx:])
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				if !rf.sendAppendEntries(server, &args, &reply) {
					return
				}

				var msgs []raftapi.ApplyMsg

				rf.mu.Lock()
				if rf.role != leader {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					rf.becomeFollowerLocked(reply.Term)
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					oldCommit := rf.commitIndex
					rf.updateCommitIndexLocked()
					if rf.commitIndex > oldCommit {
						msgs = rf.collectApplyMsgsLocked()
					}
					rf.mu.Unlock()

					rf.sendApplyMsgs(msgs)
					return
				}

				if rf.nextIndex[server] > 1 {
					rf.nextIndex[server]--
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		role := rf.role
		elapsed := time.Since(rf.lastResetElectionTime)
		timeout := rf.electionTimeout
		rf.mu.Unlock()

		if role == leader {
			rf.broadcastHeartbeat()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if elapsed >= timeout {
			rf.startElection()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh

	rf.role = follower
	rf.currentTerm = 0
	rf.vodtedFor = -1

	// 哨兵日志，方便处理 index=0
	rf.log = []LogEntry{{Term: 0}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}

	rf.resetElectionTimerLocked()

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
