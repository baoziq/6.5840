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

type RoleType int

const (
	FOLLOWER RoleType = iota
	CANDIDATE
	LEADER
)

// type Log struct {
// 	term    int
// 	command byte
// }

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persisten state
	currentTerm int
	votedFor    int
	// log         []Log
	// volatile state
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	role            RoleType
	electionTimeout time.Duration
	lastActiveTime  time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LEADER
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
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int // server's current term
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	// Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int // current term
	Success bool
}

type VoteResult struct {
	voteGranted bool
	term        int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}
	// if args.LastLogIndex < len(rf.log) {
	// 	return
	// }
	// if args.LastLogTerm < rf.log[len(rf.log)].term {
	// 	return
	// }
	// log.Printf("server %v's voteFor is %v\n", rf.me, rf.votedFor)
	reply.VoteGranted = true

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// if len(args.Entries) == 0 {
	// 	rf.role = FOLLOWER
	// 	rf.lastActiveTime = time.Now()
	// 	return
	// }
	// log.Printf("appendEntries befor lock\n")
	// // log.Printf("appendEntries after lock\n")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	// log.Printf("appendEntries after lock\n")
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Success = true
		rf.role = FOLLOWER
		rf.votedFor = -1
		// log.Printf("server %v has received %v's AppendEntries and become follower\nserver %v's term is %v, args's term is %v", rf.me, args.LeaderId, rf.me, rf.currentTerm, args.Term)
		rf.lastActiveTime = time.Now()
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

func (rf *Raft) sendAppendEntriesVote(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	curTerm := rf.currentTerm
	rf.lastActiveTime = time.Now()
	// ch := make(chan VoteResult, len(rf.peers)-1)
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	votes := 1
	// log.Printf("%v starts election term is %v\n", rf.me, rf.currentTerm)
	// curRole := rf.role
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// log.Printf("server %v send to server %v\n", rf.me, i)
		go func(i int) {
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				// log.Printf("server %v send to server %v error\n", rf.me, i)
				return
			}
			// log.Printf("server %v received server %v's vote reply, reply's term is %v", rf.currentTerm, i, reply.Term)
			rf.mu.Lock()

			// 对方的 term 更大
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = FOLLOWER
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			}

			// 这个 RPC reply 已经过期
			if curTerm != rf.currentTerm || rf.role != CANDIDATE {
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				votes++

				if votes > len(rf.peers)/2 {
					rf.role = LEADER

					// log.Printf("server %v has become leader in term %v\n", rf.me, rf.currentTerm)

					rf.mu.Unlock()

					rf.heartbeat()
					return
				}
			}

			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}
	// curRole := rf.role
	curTerm := rf.currentTerm
	curMe := rf.me
	curCommmitIndex := rf.commitIndex
	rf.lastActiveTime = time.Now()
	rf.mu.Unlock()
	// log.Printf("server %v has started heartbeat\n", curMe)
	// log.Printf("server %v's role is %v", curMe, curRole)

	for i := 0; i < len(rf.peers); i++ {
		if i == curMe {
			continue
		}
		// log.Printf("server %v send heartbeat to server %v\n", curMe, i)
		go func(i int) {
			args := AppendEntriesArgs{
				Term:     curTerm,
				LeaderId: curMe,
				// Entries:      nil,
				LeaderCommit: curCommmitIndex,
			}
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntriesVote(i, &args, &reply)
			// log.Printf("server %v send heartbeat to server %v successfully \n", curMe, i)
			if !ok {
				// log.Printf("sendAppendEntries error\n")
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.role = FOLLOWER
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) tickerHeartbeat() {
	for true {
		rf.heartbeat()
		time.Sleep(100 * time.Millisecond)
	}
}
func (rf *Raft) ticker() {
	for true {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		elapsed := time.Since(rf.lastActiveTime)
		curElectionTimeout := rf.electionTimeout
		// curMe := rf.me
		rf.mu.Unlock()
		// log.Printf("ticker started\n")
		if elapsed > curElectionTimeout {

			rf.startElection()
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
	rf.currentTerm = 0
	rf.votedFor = -1
	// rf.log = make([]Log, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.role = FOLLOWER
	rf.electionTimeout = time.Duration(400+(rand.Int63()%300)) * time.Millisecond
	rf.lastActiveTime = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.tickerHeartbeat()
	return rf
}
