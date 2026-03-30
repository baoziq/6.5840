package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"

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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role                  Role
	lastResetElectionTime time.Time
	// persistent state
	currentTerm int
	vodtedFor   int
	log         []LogEntry
	// volatile state
	commitIndex int
	lastApplied int
	// leader
	nextIndex  []int // 给server发送的日志索引
	matchIndex []int
	applyChan  chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // 本次要发送entries的前一个索引，nextIndex[i] - 1
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	// term检查
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.vodtedFor = -1
		rf.role = follower
	}

	// 一致性检查
	// index := args.LastLogIndex
	cur_term := args.LastLogTerm

	myIndex := len(rf.log) - 1
	myTerm := rf.log[myIndex].Term

	if myTerm >= cur_term {
		return
	}
	if rf.vodtedFor == -1 || rf.vodtedFor == args.CandidateId {
		rf.vodtedFor = args.CandidateId
		rf.lastResetElectionTime = time.Now()
		reply.VoteGranted = true
	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntriesVote(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true
	reply.Term = rf.currentTerm
	// term检查
	if args.Term < rf.currentTerm {
		reply.Success = false
		return

	}
	// 一致性检查
	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	// 转follower
	rf.currentTerm = args.Term
	rf.vodtedFor = -1
	rf.role = follower
	rf.lastResetElectionTime = time.Now()

	// 添加日志
	for i, entry := range args.Entries {
		resIndex := args.PrevLogIndex + i + 1
		if resIndex < len(rf.log) {
			if rf.log[resIndex].Term != entry.Term {
				rf.log = rf.log[:resIndex]
				rf.log = append(rf.log, args.Entries[i:]...)
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntriesVote", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	if rf.role != leader {
		return -1, rf.currentTerm, false
	}
	term := rf.currentTerm
	newLogEntry := LogEntry{
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, newLogEntry)
	index := len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.mu.Unlock()

	// 异步？
	rf.broadcastHeartbeat()

	return index, term, true

}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.role = candidate
	rf.currentTerm++
	rf.vodtedFor = rf.me
	termStarted := rf.currentTerm
	rf.lastResetElectionTime = time.Now()
	votesReceived := 1
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.log[lastLogIndex].Term

			args := RequestVoteArgs{
				Term:         termStarted,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != candidate || rf.currentTerm != termStarted {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = follower
				rf.vodtedFor = -1
				rf.lastResetElectionTime = time.Now()
				return
			}
			if reply.VoteGranted {
				votesReceived++
				if votesReceived > len(rf.peers)/2 && rf.role == candidate {
					rf.role = leader
					// 把所有server的nextIndex和matchIndex都初始化
					for tmp := range rf.peers {
						rf.nextIndex[tmp] = len(rf.log)
						rf.matchIndex[tmp] = 0
						rf.matchIndex[rf.me] = len(rf.log) - 1
					}

				}
			}

		}(i)
	}

}

func (rf *Raft) updateCommitIndex() {
	for N := len(rf.log) - 1; N >= rf.commitIndex+1; N-- {
		sum := 0
		for server := range rf.matchIndex {
			if server >= N {
				sum++
			}
		}
		if N >= len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
			rf.commitIndex = N
			break
		}
	}
}

func (rf *Raft) applyCommittedEntries() {
	if rf.lastApplied >= rf.commitIndex {
		return
	}
	for i := rf.lastApplied + 1; i < rf.commitIndex; i++ {

	}
}

func (rf *Raft) broadcastHeartbeat() {
	// leader
	rf.mu.Lock()
	if rf.role != leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			for {
				entries := make([]LogEntry, 0)
				prevLogIndex := rf.nextIndex[server] - 1

				if len(rf.log)-1 > prevLogIndex {
					cur_entries := rf.log[prevLogIndex+1:]
					entries = append(entries, cur_entries...)
				}

				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.log[prevLogIndex].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}

				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					return
				}

				// 判断follower是否接受
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = follower
					rf.lastResetElectionTime = time.Now()
					rf.vodtedFor = -1
					return
				}

				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					go func() {

					}()
					break
				} else {
					// 前面已经判断了term，走到这里success为false，一定是follower在nextIndex[i]位置上没有日志导致的
					// 回退
					rf.nextIndex[server] -= 1
					// 重发

				}
			}

		}(i)
	}
}

func (rf *Raft) getTimeout() time.Duration {
	// return 300 + rand()
	n := rand.Intn(300)
	return time.Duration(n)*time.Millisecond + 300*time.Millisecond
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		role := rf.role
		elapsed := time.Since(rf.lastResetElectionTime)
		timeout := rf.getTimeout()
		rf.mu.Unlock()

		if role != leader && elapsed >= timeout {
			rf.startElection()
		}
		if role == leader {
			rf.broadcastHeartbeat()
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.role = follower
	rf.currentTerm = 0
	rf.lastResetElectionTime = time.Now()
	rf.vodtedFor = -1 // 没有投票
	rf.applyChan = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = make([]LogEntry, 0)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
