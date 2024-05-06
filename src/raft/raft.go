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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// First, handle invalid RequestVote RPC
	// Invalid if cand's term is lower than ours
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// Otherwise, candidate (aka RPC requester)'s term is >= ours

	// if requester's term is higher, reset votedFor
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}

	// Update our term, set reply.Term to it
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	// Next, vote as appropriate
	// TODO: (3B) Add AND: and candidate's log is at least as up-to-date as receiver's log
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// record HB iff we vote
		rf.recentHeartbeatReceived = true

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true

		// If we're voting yes for another (not self) node, downgrade self to follower
		if rf.me != args.CandidateId {
			rf.state = followerNode
		}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// First, handle invalid AppendEntries RPC
	// if sender's term is less than receiver's, fail and set reply.Term = the higher receiver's
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// Otherwise, RPC requester is seen as leader, as it's term is >= ours
	// Update our term, set reply.Term to it, record heartbeat and make ourself a follower
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	rf.recentHeartbeatReceived = true
	if rf.me != args.LeaderId {
		// Unless we're sending to ourself (in which case we'd like to remain the leader), update our state to follower
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

// A ticker function that handlers the election process
func (rf *Raft) ticker() {

	// Exit when killed
	for !rf.killed() {

		// Choose new electionTimeout interval each period so an unlucky set of choices don't haunt us for whole life of node
		// electionTimeout will be between 800 & 1200 ms
		timeoutInterval := 800 + (rand.Int63() % 400)

		// First wait, then switch
		time.Sleep(time.Duration(timeoutInterval) * time.Millisecond)

		rf.mu.Lock()
		switch rf.state {
		case followerNode:
			// if a follower has not received a HB recently, go directly to candidate state
			if !rf.recentHeartbeatReceived {
				rf.state = candidateNode
				rf.mu.Unlock()
				continue
			} else {
				// if we have gotten a recent HB, reset it, proceed to wait again
				rf.recentHeartbeatReceived = false
			}
		case candidateNode:
			// First time around, we'll get here after becoming cand. and immediately coming here. !rf.recHBRec will still be true, so go startElec.
			// Then, wait. Hopefully, while we're waiting, we'll vote for self & receive HB. We could still be cand, but no new election spawned. Wait again, maybe by next time around we'll win and be leader
			// But maybe network failure, we won't vote for self so no HB received. we'll still be cand - go start another election.
			if !rf.recentHeartbeatReceived {
				go rf.startElection()
			} else {
				// if we have received a recent HB, reset it, proceed to wait again
				rf.recentHeartbeatReceived = false
			}
		case leaderNode:
			rf.sendHeartbeatsToAllNodes()
		}
		rf.mu.Unlock()
	}
}

// called by a candidate rf server. lock not held
func (rf *Raft) startElection() {

	rf.mu.Lock()
	rf.currentTerm++
	rf.recentHeartbeatReceived = false
	rf.mu.Unlock()

	//rf.votedFor = rf.me
	yesVotes := 0
	voteCount := 0
	var voteMutex sync.Mutex
	cond := sync.NewCond(&voteMutex)

	// Spawn len(rf.peers) goroutines, that each request a vote from a different node
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.votedFor = rf.me
			yesVotes++
		} else {
			go func(nodeIdx int) {
				// Loop till we make a successful RPC requestVote call and get either a yes or no vote
				for !rf.killed() {

					rf.mu.Lock()
					var reply RequestVoteReply
					args := RequestVoteArgs{rf.currentTerm, rf.me}
					rf.mu.Unlock()

					// Sender (rf) does not hold lock here
					rpcOk := rf.sendRequestVote(nodeIdx, &args, &reply)

					voteMutex.Lock()
					defer voteMutex.Unlock()

					// 2 outcomes for RPC call:
					// 1. success, receive either yes or no vote
					// 2. RPC did not go through. If this is because follower is down, we should retry later so it has a chance to come back up
					//// What if our connection is down? Still don't demote to follower until we demote self to follower

					if rpcOk {
						rf.mu.Lock()
						// if RPC recipient's term is higher than this candidate's term, this cand --> becomes a follower
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = followerNode
						} else if reply.VoteGranted {
							yesVotes++
						}
						rf.mu.Unlock()

						voteCount++
						cond.Broadcast()
						return
					}
				}
			}(i)
		}
	}

	voteMutex.Lock()

	majority := len(rf.peers)/2 + 1

	for yesVotes < majority && voteCount != len(rf.peers) {
		cond.Wait()
	}

	if yesVotes >= majority {
		rf.mu.Lock()
		rf.state = leaderNode
		rf.mu.Unlock()
	}

	voteMutex.Unlock()
}

// TODO: incorporate the 'idle' factor into this. Should only send when (leader? is) idle
func (rf *Raft) sendHeartbeatsToAllNodes() {
	// Can send 10 RPCs a second, about 1 every 100 seconds
	// time between heartbeats should be between 100 & 300 ms
	timeBetweenHeartbeats := 100 + (rand.Int63() % 200)

	for i := 0; i < len(rf.peers); i++ {
		go rf.sendHeartbeatsToNode(i, timeBetweenHeartbeats)
	}
}

// periodically send heartbeat, process response, wait, repeat while not killed
// rf's lock not held when called
func (rf *Raft) sendHeartbeatsToNode(nodeIdx int, timeBetweenHeartbeats int64) {
	for !rf.killed() {
		rf.mu.Lock()
		var reply AppendEntriesReply
		args := AppendEntriesArgs{rf.currentTerm, rf.me}
		rf.mu.Unlock()

		// Sender (rf)'s lock not held
		rpcOk := rf.sendAppendEntries(nodeIdx, &args, &reply)

		if !rpcOk {
			// on network failure, downgrade leader to follower, stop sending heartbeats
			rf.mu.Lock()
			rf.state = followerNode
			rf.mu.Unlock()
			return
		} else if !reply.Success {
			// if RPC goes through but reply.Success? is a fail, that means sender's term was less than receivers. stop sending heartbeats
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.state = followerNode
			rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = followerNode
	rf.currentTerm = 0
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
