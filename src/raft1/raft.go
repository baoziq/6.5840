package raft

import (
	"bytes"
	"fmt"
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
	applyCond *sync.Cond

	role Role

	lastResetElectionTime time.Time
	electionTimeout       time.Duration

	// persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// leader state
	nextIndex  []int
	matchIndex []int

	applyChan chan raftapi.ApplyMsg

	// snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	pendingSnapshot   *raftapi.ApplyMsg
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

func (rf *Raft) lastLogIndexLocked() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) toSliceIndex(logIndex int) int {
	return logIndex - rf.lastIncludedIndex
}

func (rf *Raft) termAtLocked(logIndex int) int {
	if logIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[rf.toSliceIndex(logIndex)].Term
}

func (rf *Raft) persist() {
	// 3C
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.Save(data, rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// 3C
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIndex int
	var lastTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&log) != nil || d.Decode(&votedFor) != nil || d.Decode(&lastIndex) != nil || d.Decode(&lastTerm) != nil {
		fmt.Println("Error: readPersist failed")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIndex
		rf.lastIncludedTerm = lastTerm
		rf.mu.Unlock()
	}
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 3D
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.lastApplied {
		return
	}

	term := rf.termAtLocked(index)
	sliceIdx := rf.toSliceIndex(index)

	newLog := make([]LogEntry, 1, len(rf.log)-sliceIdx)
	newLog[0] = LogEntry{Term: term}
	newLog = append(newLog, rf.log[sliceIdx+1:]...)

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.log = newLog
	rf.snapshot = append([]byte(nil), snapshot...)
	rf.persist()
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) getTimeout() time.Duration {
	n := rand.Intn(400)
	return time.Duration(n)*time.Millisecond + 600*time.Millisecond
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.lastResetElectionTime = time.Now()
	rf.electionTimeout = rf.getTimeout()
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
	rf.role = follower
	rf.resetElectionTimerLocked()
}

func (rf *Raft) lastLogIndexTermLocked() (int, int) {
	lastIndex := rf.lastLogIndexLocked()
	lastTerm := rf.termAtLocked(lastIndex)
	return lastIndex, lastTerm
}

func (rf *Raft) collectApplyMsgsLocked() []raftapi.ApplyMsg {
	if rf.lastApplied >= rf.commitIndex {
		return nil
	}

	start := rf.lastApplied + 1
	end := rf.commitIndex
	msgs := make([]raftapi.ApplyMsg, 0, end-rf.lastApplied)
	for i := start; i <= end; i++ {
		msgs = append(msgs, raftapi.ApplyMsg{
			CommandValid: true,
			// Command:      rf.log[i].Command,
			Command:      rf.log[rf.toSliceIndex(i)].Command,
			CommandIndex: i,
		})
	}
	rf.lastApplied = end
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
	for N := rf.lastLogIndexLocked(); N > rf.commitIndex; N-- {
		if rf.termAtLocked(N) != rf.currentTerm {
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
			rf.applyCond.Signal()
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

	if upToDate && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimerLocked()
		reply.VoteGranted = true
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) AppendEntriesVote(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictIndex = rf.lastIncludedIndex + 1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	} else {
		rf.role = follower
		rf.resetElectionTimerLocked()
	}
	reply.Term = rf.currentTerm

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}
	if args.PrevLogIndex > rf.lastLogIndexLocked() {
		reply.ConflictIndex = rf.lastLogIndexLocked() + 1
		return
	}
	if rf.termAtLocked(args.PrevLogIndex) != args.PrevLogTerm {
		conflictTerm := rf.termAtLocked(args.PrevLogIndex)
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = args.PrevLogIndex
		for reply.ConflictIndex > rf.lastIncludedIndex+1 && rf.termAtLocked(reply.ConflictIndex-1) == conflictTerm {
			reply.ConflictIndex--
		}
		return
	}
	logChanged := false
	for i, entry := range args.Entries {
		logIndex := args.PrevLogIndex + 1 + i

		if logIndex <= rf.lastLogIndexLocked() {
			if rf.termAtLocked(logIndex) != entry.Term {
				rf.log = rf.log[:rf.toSliceIndex(logIndex)]
				rf.log = append(rf.log, args.Entries[i:]...)
				logChanged = true
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			logChanged = true
			break
		}
	}
	if logChanged {
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, rf.lastLogIndexLocked())
		rf.applyCond.Signal()
	}
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntriesVote", args, reply)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
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

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	newLog := []LogEntry{{Term: args.LastIncludedTerm}}
	if args.LastIncludedIndex <= rf.lastLogIndexLocked() && rf.termAtLocked(args.LastIncludedIndex) == args.LastIncludedTerm {
		sliceIdx := rf.toSliceIndex(args.LastIncludedIndex)
		newLog = append(newLog, rf.log[sliceIdx+1:]...)
	}
	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = append([]byte(nil), args.Data...)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	msg := &raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      append([]byte(nil), rf.snapshot...),
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.pendingSnapshot = msg
	rf.persist()
	rf.applyCond.Signal()
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) findLastIndexOfTermLocked(term int) int {
	for i := rf.lastLogIndexLocked(); i >= 1; i-- {
		if rf.termAtLocked(i) == term {
			return i
		}
	}
	return -1
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
	rf.persist()
	// index := len(rf.log) - 1
	index := rf.lastLogIndexLocked()
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
	rf.votedFor = rf.me
	rf.persist()
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
					lastIdx := rf.lastLogIndexLocked()
					for j := range rf.peers {
						rf.nextIndex[j] = lastIdx + 1
						rf.matchIndex[j] = rf.lastIncludedIndex
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
	for {
		rf.mu.Lock()
		for rf.pendingSnapshot == nil && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			rf.pendingSnapshot = nil
			rf.mu.Unlock()
			rf.applyChan <- msg
			continue
		}
		msgs := rf.collectApplyMsgsLocked()
		rf.mu.Unlock()

		rf.sendApplyMsgs(msgs)
	}
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
				if nextIdx <= rf.lastIncludedIndex {
					args := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Data:              append([]byte(nil), rf.snapshot...),
					}
					rf.mu.Unlock()

					reply := InstallSnapshotReply{}
					if !rf.sendInstallSnapshot(server, &args, &reply) {
						return
					}

					rf.mu.Lock()
					if rf.role != leader {
						rf.mu.Unlock()
						return
					}
					if args.Term != rf.currentTerm {
						rf.mu.Unlock()
						return
					}
					if reply.Term > rf.currentTerm {
						rf.becomeFollowerLocked(reply.Term)
						rf.mu.Unlock()
						return
					}
					if rf.matchIndex[server] < args.LastIncludedIndex {
						rf.matchIndex[server] = args.LastIncludedIndex
					}
					if rf.nextIndex[server] < args.LastIncludedIndex+1 {
						rf.nextIndex[server] = args.LastIncludedIndex + 1
					}
					rf.mu.Unlock()
					return
				}
				prevLogIndex := nextIdx - 1
				prevLogTerm := rf.termAtLocked(prevLogIndex)
				entries := make([]LogEntry, len(rf.log[rf.toSliceIndex(nextIdx):]))
				copy(entries, rf.log[rf.toSliceIndex(nextIdx):])
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

				rf.mu.Lock()
				if rf.role != leader {
					rf.mu.Unlock()
					return
				}
				if args.Term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					rf.becomeFollowerLocked(reply.Term)
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					newMatch := args.PrevLogIndex + len(args.Entries)
					if newMatch > rf.matchIndex[server] {
						rf.matchIndex[server] = newMatch
						rf.nextIndex[server] = newMatch + 1
					}
					rf.updateCommitIndexLocked()
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[server] == nextIdx {
					if reply.ConflictTerm != -1 {
						if idx := rf.findLastIndexOfTermLocked(reply.ConflictTerm); idx != -1 {
							rf.nextIndex[server] = idx + 1
						} else {
							rf.nextIndex[server] = reply.ConflictIndex
						}
					} else {
						rf.nextIndex[server] = reply.ConflictIndex
					}
					if rf.nextIndex[server] < 1 {
						rf.nextIndex[server] = 1
					}
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
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.role = follower
	rf.currentTerm = 0
	rf.votedFor = -1

	// 哨兵日志，方便处理 index=0
	rf.log = []LogEntry{{Term: 0}}

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapshot = nil

	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	lastIdx := rf.lastLogIndexLocked()
	for i := range peers {
		rf.matchIndex[i] = rf.lastIncludedIndex
		rf.nextIndex[i] = lastIdx + 1
	}

	rf.resetElectionTimerLocked()

	go rf.ticker()
	go rf.applyCommittedEntries()

	return rf
}
