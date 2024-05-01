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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type serverType int

const (
	leaderNode serverType = iota
	candidateNode
	followerNode
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state                   serverType // needs locking
	recentHeartbeatReceived bool       // needs locking

	// TODO (3C) Must be updated into persister before responding to RPCs
	currentTerm int // latest term this server has seen // needs locking
	votedFor    int // candidateId that received vote of this server in current term (or -1 if none)
}

func (rf *Raft) lockAndDebug(ctx string) {
	//DPrintf("[%v]'s mu about to be locked in %v", rf.me, ctx)
	rf.mu.Lock()
}

func (rf *Raft) unlockAndDebug(ctx string) {
	//DPrintf("[%v]'s mu about to be unlocked in %v", rf.me, ctx)
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lockAndDebug("GetState")
	defer rf.unlockAndDebug("GetState")
	return rf.currentTerm, rf.state == leaderNode
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// Your data here (3B).
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
// Executed by receiver when a vote is requested by args.CandidateId
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// sender does not have lock here

	// For the majority of this function to work, rf must be locked
	// Then, it must return to unlocked state
	// The sender does not hold its own lock
	// Regardless of receiving from self/other, we must lock/unlock

	DPrintf("%v requests vote from %v", args.CandidateId, rf.me)

	rf.lockAndDebug("RequestVote from self")
	defer rf.unlockAndDebug("RequestVote from self")

	// First, handle invalid RequestVote RPC
	// Invalid if cand's term is lower than ours
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[%v] not voting for candidate %v because cand's term < [%v]'s term", rf.me, args.CandidateId, rf.me)
		return
	}

	// Otherwise, candidate (aka RPC requester)'s term is >= ours
	// Update our term, set reply.Term to it, record heartbeat and ourselves as a follower
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	if rf.me != args.CandidateId {
		// Unless we're voting for ourself (in which case we'd like to remain a cand), downgrade to follower
		DPrintf("[%v] downgrade to follower - caller (%v) has higher (or ==) term than ours. State: %v -> follower", rf.me, args.CandidateId, rf.state)
		rf.state = followerNode
	}

	// Next, vote as appropriate
	// TODO: (3B) Add AND: and candidate's log is at least as up-to-date as receiver's log
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		DPrintf("[%v] records heartbeat on vote cast", rf.me)
		rf.recentHeartbeatReceived = true
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("[%v] grants vote to %v", rf.me, args.CandidateId)
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

type AppendEntriesArgs struct {
	Term     int // leaders term
	LeaderId int // so follower can redirect clients

	// Your code for 3B here
}

type AppendEntriesReply struct {
	Term    int  // currentTerm - used by leader to update its term & to step down if reply.Term > args.Term // TODO OR >= ?
	Success bool // true if follower contained entry matching requester's prevLog, false otherwise
	// Your code for 3B here
}

// Handler for recipient of an AppendEntries RPC call
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Sender's lock not held

	// For the majority of this function to work, rf must be locked
	// Then, it must return to unlocked state
	// The sender does not hold its own lock
	// Regardless of sending from self/other, receiver must lock/unlock

	rf.lockAndDebug("AppendEntries")
	defer rf.unlockAndDebug("AppendEntries")

	// First, handle invalid AppendEntries RPC
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("[%v] reply to AppendEntries RPC call is unsuccessful because caller (%v)'s term < [%v]'s term", rf.me, args.LeaderId, rf.me)
		return
	}

	// Otherwise, RPC requester is seen as leader, as it's term is >= ours
	// Update our term, set reply.Term to it, record heartbeat and ourself as a follower
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	DPrintf("[%v] records heartbeat: valid AE RPC from leader %v received", rf.me, args.LeaderId)
	rf.recentHeartbeatReceived = true
	if rf.me != args.LeaderId {
		// Unless we're sending to ourself (in which case we'd like to remain the leader), update our state to follower
		DPrintf("[%v] state transition after valid AppendEntries RPC from leader %v:: %v -> follower", rf.me, args.LeaderId, rf.state)
		rf.state = followerNode
	}

	// TODO: do 3B stuff
	reply.Success = true
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
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	// electionTimeout should be between 800 & 1200 ms
	electionTimeout := 800 + (rand.Int63() % 400)

	for !rf.killed() {
		// As a follower, should I become a candidate?

		// start the timeout with no recentHeartbeat received
		rf.lockAndDebug("ticker")
		rf.recentHeartbeatReceived = false
		rf.unlockAndDebug("ticker")

		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		rf.lockAndDebug("ticker, before check for timeout")
		if !rf.recentHeartbeatReceived && (rf.state == followerNode || rf.state == candidateNode) {
			DPrintf("[%v] timed out while waiting for heartbeat (votes granted, valid incoming RPCs)", rf.me)
			DPrintf("state: %v, !rf.recentHeartbeatReceived: %v", rf.state, !rf.recentHeartbeatReceived)
			rf.state = candidateNode
			rf.unlockAndDebug("ticker, before election")
			rf.startElection()
			rf.lockAndDebug("ticker, after election")
		}
		rf.unlockAndDebug("ticker, after check for timeout (and poss. election)")
	}
}

// called by a candidate rf server. lock not held
func (rf *Raft) startElection() {
	DPrintf("[%v] starting election", rf.me)

	rf.lockAndDebug("beg. of startElection")
	rf.currentTerm++
	rf.recentHeartbeatReceived = false
	rf.unlockAndDebug("beg. of startElection")

	//rf.votedFor = rf.me
	yesVotes := 0
	voteCount := 0
	var voteMutex sync.Mutex
	cond := sync.NewCond(&voteMutex) // TODO Should this be on existing mutex? I don't think so?

	// Spawn len(rf.peers) goroutines, that each request a vote from a different node
	for i := 0; i < len(rf.peers); i++ {
		go func(nodeIdx int) {
			DPrintf("[%v] requesting vote from %v", rf.me, nodeIdx)

			rf.lockAndDebug("startElection, goroutine")
			var reply RequestVoteReply
			args := RequestVoteArgs{rf.currentTerm, rf.me}
			rf.unlockAndDebug("startElection, goroutine")

			// Sender (rf) does not hold lock here
			rf.sendRequestVote(nodeIdx, &args, &reply)

			voteMutex.Lock()
			defer voteMutex.Unlock()

			rf.lockAndDebug("startElection, goroutine after RPC")
			// if RPC recipient's term is higher than this candidate's term, this cand --> becomes a follower
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = followerNode
			} else if reply.VoteGranted {
				yesVotes++
			}
			rf.unlockAndDebug("startElection, goroutine after RPC")

			voteCount++
			cond.Broadcast()
		}(i)
	}

	voteMutex.Lock()

	majority := len(rf.peers)/2 + 1

	for yesVotes < majority && voteCount != len(rf.peers) {
		cond.Wait()
	}

	if yesVotes >= majority {
		DPrintf("[%v] received %v (enough) yes votes", rf.me, yesVotes)
		rf.lockAndDebug("startElection, during promotion to leader")
		rf.state = leaderNode
		rf.unlockAndDebug("startElection, during promotion to leader")
		// Sender (rf) does not hold lock here
		rf.sendHeartbeatsToAllNodes()
	} else {
		DPrintf("[%v] only received %v yes votes (not enough)", rf.me, yesVotes)
	}

	voteMutex.Unlock()
}

// Only called by leaders
// rf's lock should not be held at time of this call
// TODO: incorporate the 'idle' factor into this. Should only send when (leader? is) idle
func (rf *Raft) sendHeartbeatsToAllNodes() {
	// Can send 10 RPCs a second, about 1 every 100 seconds
	// time between heartbeats should be between 100 & 300 ms
	timeBetweenHeartbeats := 100 + (rand.Int63() % 200)

	DPrintf("[%v], believing it's leader, will send a heartbeat every %v ms", rf.me, timeBetweenHeartbeats)

	for i := 0; i < len(rf.peers); i++ {
		go rf.sendHeartbeatToNode(i, timeBetweenHeartbeats)
	}
}

// periodically send heartbeat, process response, wait, repeat while not killed
// rf's lock not held when called
func (rf *Raft) sendHeartbeatToNode(nodeIdx int, timeBetweenHeartbeats int64) {
	for !rf.killed() {
		rf.lockAndDebug("sendHBToNode")
		var reply AppendEntriesReply
		args := AppendEntriesArgs{rf.currentTerm, rf.me}
		rf.unlockAndDebug("sendHBToNode")

		DPrintf("[%v] sends heartbeat to %v", rf.me, nodeIdx)

		// Sender (rf)'s lock not held
		rf.sendAppendEntries(nodeIdx, &args, &reply)

		// RPC response
		// follower's term is higher than (this) the leader's term?
		// args.Term < receiver's currentTerm
		if !reply.Success {
			rf.lockAndDebug("sendHBToNode, not success")
			DPrintf("[%v] AE RPC call to %v unsuccessfull. [%v] being downgraded from %v to follower", rf.me, nodeIdx, rf.me, rf.state)
			rf.currentTerm = reply.Term
			rf.state = followerNode
			rf.unlockAndDebug("sendHBToNode, not success")
			return
		}

		time.Sleep(time.Duration(timeBetweenHeartbeats) * time.Millisecond)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.lockAndDebug("make")
	defer rf.unlockAndDebug("make")
	rf.state = followerNode
	rf.currentTerm = 0
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
