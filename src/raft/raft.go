package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	RoleFollower  = 0
	RoleCandidate = 1
	RoleLeader    = 2
	RoleExit      = 3

	heartbeatIntervalMin = 200
	heartbeatIntervalMax = 500

	electionTimeoutMin = 200
	electionTimeoutMax = 500
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term int
	Data interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role int
	term int

	voteFor   int
	voteCount int

	heartbeatChan chan int

	electWin chan int

	log []LogEntry
}

func (rf *Raft) unsafeGetLastLogId() int {
	return len(rf.log) - 1
}

func (rf *Raft) unsafeGetLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.term, rf.role == RoleLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

func (rf *Raft) generateElectionTimeout() time.Duration {
	rand.Seed(time.Now().Unix()*10 + int64(rf.me))

	randomSecond := rand.Intn(electionTimeoutMax - electionTimeoutMin)
	return time.Duration(electionTimeoutMin+randomSecond) * time.Millisecond
}

func (rf *Raft) generateHeartbeatInterval() time.Duration {
	rand.Seed(time.Now().Unix()*10 + int64(rf.me))

	randomSecond := rand.Intn(heartbeatIntervalMax - heartbeatIntervalMin)
	return time.Duration(heartbeatIntervalMin+randomSecond) * time.Millisecond
}

func (rf *Raft) unsafeChangeRole(newRole int) {
	rf.role = newRole
}

func (rf *Raft) candidateLoop() bool {
	electionTimeoutTimer := time.After(rf.generateElectionTimeout())
	select {
	case <-rf.heartbeatChan:
		rf.mu.Lock()
		rf.unsafeChangeRole(RoleFollower)
		rf.mu.Unlock()
	case <-rf.electWin:
		rf.mu.Lock()
		rf.unsafeChangeRole(RoleLeader)
		rf.mu.Unlock()
	case <-electionTimeoutTimer:
		return false
	}
	return true
}

type AppendEntriesArgs struct {
	Term    int
	Master  int
	Entries []LogEntry
}

type AppendEntriesReply struct {
	Term int
	Ack  bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.heartbeatChan <- rf.term

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Ack = false
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term

		rf.unsafeChangeRole(RoleFollower)
		rf.voteFor = -1
	}

	reply.Term = rf.term
	reply.Ack = true
}

func (rf *Raft) leaderLoop() {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:   rf.term,
		Master: rf.me,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.role == RoleLeader {
			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) followerLoop() {
	select {
	case <-rf.heartbeatChan:
		//DPrintf("[%d] heartbeat package received %d \n", rf.me, pkg)
	case <-time.After(rf.generateHeartbeatInterval()):
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.unsafeChangeRole(RoleCandidate)
	}
}

func (rf *Raft) startElect() {
	DPrintf("[%d] Start Elect, term = %d \n", rf.me, rf.term)

	rf.mu.Lock()
	rf.term++
	rf.voteCount = 1
	rf.voteFor = rf.me
	args := &RequestVoteArgs{
		Id:           rf.me,
		Term:         rf.term,
		LastLogTerm:  rf.unsafeGetLastLogTerm(),
		LastLogIndex: rf.unsafeGetLastLogId(),
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.role == RoleCandidate {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Id           int
	Term         int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term    int
	Granted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		DPrintf("[%d] received request vote request, not granted because of low term \n", rf.me)
		reply.Granted = false
		reply.Term = rf.term
		return
	}

	if args.Term > rf.term {
		DPrintf("[%d] received request vote request, term is higher than now \n", rf.me)
		rf.term = args.Term
		rf.unsafeChangeRole(RoleFollower)
		rf.voteFor = -1
	}

	if rf.voteFor == -1 || rf.voteFor == args.Id {
		DPrintf("[%d] granted vote for %d \n", rf.me, args.Id)
		reply.Granted = true
		rf.voteFor = args.Id
	} else {
		DPrintf("[%d] vote fail for %d \n", rf.me, args.Id)
		reply.Granted = false
		reply.Term = rf.term
	}

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("[%d] Sent request vote rpc to server %d \n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleCandidate || rf.term != args.Term {
		return ok
	}

	if reply.Term > rf.term {
		rf.unsafeChangeRole(RoleFollower)
		rf.term = reply.Term
		rf.voteFor = -1
		return ok
	}

	if reply.Granted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.unsafeChangeRole(RoleLeader)
			rf.electWin <- rf.term
		}
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//DPrintf("[%d] Sending hearbeat to %d \n", rf.me, server)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleLeader || rf.term != args.Term {
		return ok
	}

	if reply.Term > rf.term {
		rf.unsafeChangeRole(RoleFollower)
		rf.term = reply.Term
		rf.voteFor = -1
		return ok
	}

	if reply.Ack {

	}

	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) run() {
	for {
		switch rf.role {
		case RoleLeader:
			//DPrintf("[%d] is Leader, run leader loop \n", rf.me)
			rf.leaderLoop()
			<-time.After(time.Millisecond * 120)
		case RoleFollower:
			//DPrintf("[%d] is Follower, run follower loop \n", rf.me)
			rf.followerLoop()
		case RoleCandidate:
			//DPrintf("[%d] is Candidate, run candidate loop \n", rf.me)
			for {
				rf.startElect()
				success := rf.candidateLoop()
				if success {
					break
				}
				DPrintf("[%d] Elect fail, reruning \n", rf.me)
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		role:      RoleFollower,

		voteFor:       -1,
		heartbeatChan: make(chan int, 10),
		electWin:      make(chan int, 10),
	}

	go rf.run()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
