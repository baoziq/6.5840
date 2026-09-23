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
	"sort"
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

type Log struct {
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
	// persisten state
	currentTerm int
	votedFor    int
	log         []Log
	// volatile state
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	role            RoleType
	electionTimeout time.Duration
	lastActiveTime  time.Time
	applyCond       *sync.Cond
	leaderCond      *sync.Cond
	applyCh         chan raftapi.ApplyMsg
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
	Entries      []Log
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
	// log.Printf("server %v receive candidate %v's requestVote\n", rf.me, args.CandidateId)
	if args.Term < rf.currentTerm {
		// log.Printf("candidate %v's Term < %v's Term, reject\n", args.CandidateId, rf.me)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
		// reply.VoteGranted = true
		reply.Term = rf.currentTerm
		// log.Printf("candidate %v's Term > %v's Term, vote\n", args.CandidateId, rf.me)
	}
	myLastIndex := len(rf.log) - 1
	myLastTerm := rf.log[myLastIndex].Term
	upToDate := args.LastLogTerm > myLastTerm || (args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)
	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	if canVote && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	// log.Printf("server %v receive AE from server %v\n", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm {
		// log.Printf("For server %v, args.Term is %v, rf.curTerm is %v\n", rf.me, args.Term, rf.currentTerm)
		return
	}
	rf.lastActiveTime = time.Now()
	reply.Success = true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
		// log.Printf("server %v has received %v's AppendEntries and become follower\nserver %v's term is %v, args's term is %v", rf.me, args.LeaderId, rf.me, rf.currentTerm, args.Term)

	}

	// log.Printf("server %v receive AppendEntries from server %v\n args.PrevLogIndex is %v, args.PrevLogTerm is %v", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		// log.Printf("server %v receive AppendEntries from server %v\n args.PrevLogIndex is %v, args.PrevLogTerm is %v", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		// log.Printf("rf.log[args.PrevLogIndex].Term != args.PrevLogTerm\n")
		return
	}
	// log.Printf("For server %v, args.LeaderCommit is %v, rf.commitIndex is %v\n", rf.me, args.LeaderCommit, rf.commitIndex)

	// log.Printf("reply is Success\n")
	// log.Printf("args.Entries is %v", args.Entries)
	for i := 0; i < len(args.Entries); i++ {
		cur := i + args.PrevLogIndex + 1
		if len(rf.log) == cur {
			rf.log = append(rf.log, args.Entries[i:]...)
			// log.Printf("follower %v's log is %v", rf.me, rf.log)
			break
		}
		if rf.log[cur].Term != args.Entries[i].Term {
			rf.log = rf.log[:cur]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		// log.Printf("args.LeaderCommit %v > rf.commitIndex %v", args.LeaderCommit, rf.commitIndex)
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		// log.Printf("server %v's commitIndex is %v\n", rf.me, rf.commitIndex)
		rf.applyCond.Broadcast()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
	isLeader := false

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.role == LEADER
	index = len(rf.log)
	if rf.role == LEADER {
		// log.Printf("server %v is %v now, call Start, command is %v", rf.me, rf.role, command)
		entry := Log{
			Term:    term,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		isLeader = true
	}
	// log.Printf("leader %v's log is %v\n", rf.me, rf.log)
	return index, term, isLeader
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	// curTerm := rf.currentTerm
	rf.lastActiveTime = time.Now()
	// curMe := rf.me
	// curTerm := rf.currentTerm
	// ch := make(chan VoteResult, len(rf.peers)-1)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
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
				// log.Printf("server %v send to server %v error\n", curMe, i)
				return
			}
			// log.Printf("server %v received server %v's vote reply, reply's term is %v", curMe, i, reply.Term)
			rf.mu.Lock()

			// 对方的 term 更大
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = FOLLOWER
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted && rf.role == CANDIDATE {
				votes++
				if votes > len(rf.peers)/2 {
					rf.role = LEADER
					// log.Printf("server %v has become leader in term %v\n", rf.me, rf.currentTerm)
					rf.leaderCond.Broadcast()
					rf.initLeader()
					rf.mu.Unlock()
					rf.heartbeat()
					return
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) initLeader() {
	// 本身有锁
	length := len(rf.log)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = length
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}
	// curTerm := rf.currentTerm
	curMe := rf.me
	// curCommmitIndex := rf.commitIndex
	rf.lastActiveTime = time.Now()
	rf.mu.Unlock()
	// log.Printf("server %v has started heartbeat\n", curMe)

	for i := 0; i < len(rf.peers); i++ {
		if i == curMe {
			continue
		}
		// log.Printf("server %v send heartbeat to server %v\n", curMe, i)
		go func(i int) {
			rf.mu.Lock()
			curLogIndex := rf.nextIndex[i]
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: curLogIndex - 1,
				PrevLogTerm:  rf.log[curLogIndex-1].Term,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &args, &reply)
			if !ok {
				// log.Printf("server %v send heartbeat to server %v fail\n", curMe, i)
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
	for {
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
		if elapsed > curElectionTimeout {

			rf.startElection()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) checkFollower() {
	for true {
		// log.Printf("checkFollower\n")
		rf.mu.Lock()
		for rf.role != LEADER {
			// log.Printf("server %v is not leader now, exit checkFollower\n", rf.me)
			rf.leaderCond.Wait()
		}

		// curMe := rf.me
		curTerm := rf.currentTerm
		// curPeers := rf.peers
		// curNextIndex := append([]int(nil), rf.nextIndex...)
		// curLog := append([]Log(nil), rf.log...)
		// log.Printf("leader %v starts sending AppendEntries\n", rf.me)
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			rf.mu.Lock()
			if i == rf.me {
				rf.mu.Unlock()
				continue
			}
			if rf.nextIndex[i] > len(rf.log)-1 {
				rf.mu.Unlock()
				continue
			}
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			go func(i int) {
				for {
					if rf.role != LEADER {
						return
					}
					rf.mu.Lock()
					curLogIndex := rf.nextIndex[i]
					// log.Printf("server %v is leader now, curLogIndex is %v, send to follower %v, leader's commitIndex is %v\n", curMe, curLogIndex, i, rf.commitIndex)
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: curLogIndex - 1,
						PrevLogTerm:  rf.log[curLogIndex-1].Term,
						Entries:      rf.log[curLogIndex:],
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(i, &args, &reply)
					// log.Printf("leader %v send AppendEntries to follower %v\n", curMe, i)
					if !ok {
						// log.Printf("leader %v send AppendEntries to follower %v failure\n", curMe, i)
						return
					}
					// // log.Printf("leader %v received AppendEntries reply from follower %v\n", curMe, i)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = FOLLOWER
						rf.votedFor = -1
						// log.Printf("server %v receive server %v's reply, reply's Term is %v, curTerm is %v\n", rf.me, i, reply.Term, rf.currentTerm)
						rf.mu.Unlock()
						return
					}
					if rf.role != LEADER || rf.currentTerm != curTerm {
						rf.mu.Unlock()
						return
					}
					// log.Printf("leader is %v, follower is %v receive reply is %v", rf.me, i, reply.Success)
					if reply.Success {
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						// log.Printf("leader %v received success reply from follower %v\n", rf.me, i)
						rf.mu.Unlock()
						return
					}
					// log.Printf("retry\n")
					if rf.nextIndex[i] > 1 {
						rf.nextIndex[i]--
					}
					rf.mu.Unlock()
				}
			}(i)
		}
		time.Sleep(10 * time.Millisecond)
	}

}

func (rf *Raft) applier() {
	// // log.Printf("applier\n")
	for true {
		// log.Printf("applier before lock\n")
		rf.mu.Lock()
		// log.Printf("applier\n")
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		rf.lastApplied++
		entry := rf.log[rf.lastApplied]
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		// log.Printf("server %v before send applyCh", rf.me)
		rf.applyCh <- msg
		// log.Printf("server %v after send applyCh", rf.me)
		time.Sleep(10 * time.Millisecond)
	}

}

func (rf *Raft) updateCommit() {
	for true {
		rf.mu.Lock()
		// log.Printf("updateCommit\n")
		for rf.role != LEADER {
			rf.leaderCond.Wait()
		}
		curNextIndex := append([]int(nil), rf.matchIndex...)
		sort.Ints(curNextIndex)

		curN := curNextIndex[len(rf.peers)/2+1]
		if rf.log[curN].Term == rf.currentTerm {
			rf.commitIndex = curNextIndex[len(rf.peers)/2+1]
			rf.applyCond.Broadcast()

		}
		// log.Printf("leader %v updateCommit, now curN is %v, rf.commitIndex is %v\n", rf.me, curN, rf.commitIndex)
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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
	rf.log = make([]Log, 1) // dummy entry at the index=0
	rf.log[0].Term = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = FOLLOWER
	rf.electionTimeout = time.Duration(400+(rand.Int63()%300)) * time.Millisecond
	rf.lastActiveTime = time.Now()
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.tickerHeartbeat()
	go rf.updateCommit()
	go rf.checkFollower()
	go rf.applier()
	return rf
}
