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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

type entry struct {
	Cmd  interface{}
	Term int // term when entry was received by leader
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC endpoints of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Volatile state for each server
	// state and votesCollected need locking
	state          serverType
	votesCollected []bool
	//waitingOnRPC   []bool

	// Timeout properties - don't need locking
	electionTimeout   time.Duration // if we start an election, how long to wait for RV rpcs to return and candidate to be elected before restarting it
	electionStartedAt time.Time
	heartbeatTimeout  time.Duration // time frame in which folls & cands are unhappy if it passes completely, a constant atm
	lastHeartbeat     time.Time     // time at which we, as a follower, last received an AE RPC (doesn't matter if we're the one sending it out)

	// commitIndex and lastApplied: need locking
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry executed

	// state applicable to leaders
	nextIdx  []int // for each server (or follower?), idx of next log entry to send to it
	matchIdx []int // for each server (follower?), idx of highest log entry known to be replicated on server

	// Non-volatile state - needs locking
	currentTerm int // latest term this server has seen
	votedFor    int // candidateId that received vote of this server in current term (or -1 if none)
	log         []*entry
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
// Call when rf.mu is locked
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		//DPrintf("[%v] persist: readPersist. data to read", rf.me)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []*entry
	var currentTerm int
	var votedFor int
	if d.Decode(&log) != nil ||
		d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil {
		//DPrintf("[%v] persist: readPersist. decode error", rf.me)
	} else {
		// Load persistent
		rf.log = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		//DPrintf("[%v] persist: readPersist: read {l%v, T%v, v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
	}
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
	LastLogIdx  int // idx of candidate's last log entry
	LastLogTerm int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
// Executed by receiver when a vote is requested by args.CandidateId
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// For the majority of this function to work, rf must be locked
	// Then, it must return to unlocked state
	// The sender does not hold its own lock
	// Regardless of receiving from self/other, we must lock/unlock

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("[%v] handling RV rpc request from %v", rf.me, args.CandidateId)

	// First, handle invalid RequestVote RPC
	// Invalid if cand's term is lower than ours
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[%v] early return on RV rpc request from %v: our term (%v) is greater than cand's (%v)", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	// Otherwise, candidate (aka RPC requester)'s term is >= ours

	// If our term is behind, reset votedFor and state
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.becomeFollower()
	}

	// Update our term, set reply.Term to it
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	// Next, vote as appropriate
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandLogAtLeastAsUpToDateAsVoters(args.LastLogTerm, args.LastLogIdx) {
		//DPrintf("[%v] voting for %v", rf.me, args.CandidateId)

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now()

		// If we're voting yes for another (not self) node, downgrade self to follower
		if rf.me != args.CandidateId {
			//DPrintf("[%v] is voting for %v - downgrade self to follower", rf.me, args.CandidateId)
			rf.becomeFollower()
		}

	}

	//DPrintf("[%v] persist: save. at end of RV RPC handler {l%v, T%v v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
	rf.persist()

}

// For a yes vote, the candidate's log must also be at least as up to date as the voters
// called with lock held
func (rf *Raft) isCandLogAtLeastAsUpToDateAsVoters(candLastLogTerm int, candLastLogIdx int) bool {
	// When comparing two logs, up-to-date is defined as:
	// if the last entries of each log have different terms, then the log w a later term is more up to date
	// if they have the same term, the longer log is more up to date

	var ret bool
	myLastLogTerm := rf.log[len(rf.log)-1].Term

	if candLastLogTerm != myLastLogTerm {
		ret = candLastLogTerm >= myLastLogTerm
	} else {
		ret = candLastLogIdx+1 >= len(rf.log)
	}
	return ret
}

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
	if ok {
		rf.handleRVReply(server, args, reply)
	}
	return ok
}

func (rf *Raft) handleRVReply(follower int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm {
		// First, ensure no other goroutine bumped currentTerm while this goroutine was waiting on sendRV(). If currentTerm is no longer == args.Term, -> this node is already a follower, and we skip voteCount++, even if vote granted, etc
	} else if rf.currentTerm < reply.Term {
		// if this cand's term is less than RPC recipient's term, we'll have to update our term and convert us to follower. vote will not have been granted
		DPrintf("[%v] received ok RV response, but no vote was granted as recipient's term (%v) was higher than our own (%v). Updating currentTerm: %v -> %v, becoming cand then follower", rf.me, reply.Term, rf.currentTerm, rf.currentTerm, reply.Term)
		rf.currentTerm = reply.Term
		//DPrintf("[%v] persist: save after receiving RV rpc response, updating rf.currentTerm {l%v, T%v v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
		rf.persist()
		rf.becomeCandidate()
		rf.becomeFollower()
	} else if reply.VoteGranted && rf.state == candidateNode {
		// Check to ensure we're still a candidate
		//DPrintf("[%v] received yes vote from foll %v", rf.me, follower)
		rf.votesCollected[follower] = true
		if rf.quorumVoted() {
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) quorumVoted() bool {
	votes := 0
	for _, votedForMe := range rf.votesCollected {
		if votedForMe {
			votes++
		}
	}
	return votes >= len(rf.peers)/2+1
}

type AppendEntriesArgs struct {
	Term         int // leaders term
	LeaderId     int // so follower can redirect clients
	PrevLogIdx   int // idx of log entry immediately preceding new ones (those being sent)
	PrevLogTerm  int // term of prevLogIdx entry
	Entries      []*entry
	LeaderCommit int // leader's commitIdx
}

type AppendEntriesReply struct {
	Term    int  // currentTerm - used by leader to update its term & to step down if args.Term < reply.Term
	Success bool // true if follower contained entry matching requester's prevLog, false otherwise

	LastLogIdx int // idx of follower's last entry

	// Information on conflicting entries, to help with fast (bulk) backup (a leader can decrement nextIdx for a follower by more than just 1)
	XTerm   int // conflicting entry's term
	XIdx    int // idx of first entry with that term // not first - latest?
	XLength int // length of follower's log
}

// Handler for recipient of an AppendEntries RPC call
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// early return if rpc not valid due to outdated term
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("[%v] (T%v) returning early from AE from leader %v (T%v) with %v entries due to terms", rf.me, rf.currentTerm, args.LeaderId, args.Term, len(args.Entries))
		return
	}

	// Now, we are getting an AE RPC from a valid leader

	DPrintf("[%v] got AE RPC from valid leader %v with %v entries", rf.me, args.LeaderId, len(args.Entries))

	rf.lastHeartbeat = time.Now()

	if rf.me != args.LeaderId { // in theory, we should not need to handle this call from self, but check just in case
		// Unless we're sending to ourself (in which case we'd like to remain the leader), update our state to follower
		rf.becomeFollower()
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	// Now, early exit if log is inconsistent with leaders. with extra (fast backup) information if my (foll) log is not consistent up to where args.Entries starts in leader log
	if !rf.logIsConsistentWithLeaderBeforeArgsEntries(args.PrevLogIdx, args.PrevLogTerm) {
		reply.Success = false
		// Case 3 first: Is leader's prevLogIdx out of range of our log
		if args.PrevLogIdx >= len(rf.log) {
			reply.XLength = len(rf.log)
			reply.XTerm = -1
			reply.XIdx = -1
			DPrintf("[%v] found case 3 inconsistency on AE from %v of %v entries (args.PrevLogIdx %v >= len(rf.log) %v", rf.me, args.LeaderId, len(args.Entries), args.PrevLogIdx, len(rf.log))
		} else if rf.log[args.PrevLogIdx].Term != args.PrevLogTerm {
			// Case 1, 2 are the same from follower's pov. Leader must distinguish with returned xTerm, xIdx info
			reply.XTerm = rf.log[args.PrevLogIdx].Term
			reply.XIdx = rf.getFirstIdxWithTerm(reply.XTerm)
			reply.XLength = len(rf.log) // won't be used
			DPrintf("[%v] found case 1,2 inconsistency on AE from %v of %v entries", rf.me, args.LeaderId, len(args.Entries))
		}
		return
	}

	if len(args.Entries) > 0 {
		DPrintf("[%v] processing %v entries from AE rpc - no conflicts between our log and prev log of leader found", rf.me, len(args.Entries))
	}

	// Here, my (foll) log is consistent with leader's log up to args.Entries.

	// Sanity check - we're not about to delete an already committed entry, are we???
	if rf.commitIndex > args.PrevLogIdx {
		//fmt.Printf("Occassionally we'll reach here if we're getting entries we already have. // TODO but check: if any of our entries at commitIdx or higher DO NOT exist in args.Entries, we have a problem - we cannot overwrite committed entries with anything else.
	}

	// Delete and append as necessary
	reply.Success = true

	// If !HB, delete unnecessary entries from follower & append
	// Start by nulling entries in follower at the location of first args.Entries of leader
	// aka at the args.PrevLogIdx + 1
	if len(args.Entries) > 0 {

		startClearingFollLogAt := args.PrevLogIdx + 1
		if len(rf.log)-startClearingFollLogAt > 0 {
			DPrintf("[%v] deleting %v entries, starting at idx %v\n", rf.me, len(rf.log)-startClearingFollLogAt, startClearingFollLogAt)
		}
		for j := startClearingFollLogAt; j < len(rf.log); j++ {
			rf.log[j] = nil
		}

		// Now, shorten length of rf.log
		// We already dealt with Case 3 above (follower's log is shorter than prevLogIdx), so if we reach here we will not be out of bounds
		rf.log = rf.log[0:startClearingFollLogAt]

		// Now, append
		commitIdxGtPrevLogIdx := false
		for i := 0; i < len(args.Entries); i++ {
			DPrintf("[%v] received cmd %v-%v from leader %v. Adding to log at idx %v", rf.me, args.Entries[i].Term, args.Entries[i].Cmd, args.LeaderId, len(rf.log))
			// If our commitIdx is higher than args.PrevLogIdx, then skip
			if rf.commitIndex > args.PrevLogIdx { // Not sure if this check should be here??? // TODO
				//fmt.Printf("Don't think I should reach here. Trying to append %v entries when foll's commitIdx (%v) is past leader's PrevLogIdx (%v) \n", len(args.Entries), rf.commitIndex, args.PrevLogIdx)

				commitIdxGtPrevLogIdx = true
				//continue??
				//break //?
			}
			rf.log = append(rf.log, args.Entries[i])
		}
		if commitIdxGtPrevLogIdx {
			//fmt.Printf("[%v] (foll) has commitIdx higher than idx before args.Entries. Appended %v entries to our log. Log, currently: %v\n", rf.me, len(args.Entries), printEntries(rf.log, 0))
		}
	}

	// // TODO: do this on HB? Maybe not, but then we have to check periodically?
	// Now, update commitIndex
	//DPrintf("[%v] received & appended entries from leader %v. Is args.LeaderCommit (%v) > rf.commitIdx (%v)? %v. args.PrevLogIdx: %v", rf.me, args.LeaderId, args.LeaderCommit, rf.commitIndex, args.LeaderCommit > rf.commitIndex, args.PrevLogIdx)
	if args.LeaderCommit > rf.commitIndex {

		DPrintf("[%v] updating commitIdx from %v to min(%v, %v)", rf.me, rf.commitIndex, args.LeaderCommit, len(rf.log)-1)
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		//rf.commitIndex = min(args.LeaderCommit, args.PrevLogIdx+len(args.Entries)) // TODO: I think this line is correct instead of above???

		rf.attemptApply()
	}

	// With normal exit, we've edited rf.log, rf.currentTerm (must remain persistent). Save before returning
	//DPrintf("[%v] persist: save. at end of AE RPC handler {l%v, T%v v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
	rf.persist()

}

func (rf *Raft) logIsConsistentWithLeaderBeforeArgsEntries(leaderPrevEntryIdx int, leaderPrevEntryTerm int) bool {
	DPrintf("[%v] rf log consistent with leaders clause 1: leaderPrevEntryIdx (%v) < 0?", rf.me, leaderPrevEntryIdx)
	DPrintf("[%v] rf log consistent with leaders clause 2: leaderPrevEntryIdx (%v) < len(rf.log) (%v)?", rf.me, leaderPrevEntryIdx, len(rf.log))
	if leaderPrevEntryIdx < len(rf.log) {
		DPrintf("[%v] rf log consistent with leaders clause 3: rf.log[leaderPrevEntryIdx].Term (%v) == leaderPrevEntryTerm (%v)?", rf.me, rf.log[leaderPrevEntryIdx].Term, leaderPrevEntryTerm)
	}
	ret := leaderPrevEntryIdx < 0 || (leaderPrevEntryIdx < len(rf.log) && rf.log[leaderPrevEntryIdx].Term == leaderPrevEntryTerm)
	DPrintf("[%v] is log consistent with leader log before args.Entries?: %v", rf.me, ret)
	return ret
}

//func (rf *Raft) applyPeriodically() {
//	for !rf.killed() {
//		rf.mu.Lock()
//		if rf.commitIndex > rf.lastApplied {
//			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
//				DPrintf("[%v] sending applyMsg idx %v. commitIdx (%v) > lastApplied (%v)", rf.me, i, rf.commitIndex, rf.lastApplied)
//				rf.mu.Unlock() // TODO remove this set?
//				rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Cmd, CommandIndex: i}
//				rf.mu.Lock()
//				rf.lastApplied++
//			}
//		}
//		rf.mu.Unlock()
//		time.Sleep(time.Duration(50) * time.Millisecond)
//	}
//}

func (rf *Raft) attemptApply() {
	// if commitIdx > lastApplied, send to apply channel
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			DPrintf("[%v] sending applyMsg idx %v. commitIdx (%v) > lastApplied (%v)", rf.me, i, rf.commitIndex, rf.lastApplied)
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Cmd, CommandIndex: i}
			rf.lastApplied++
		}
	}
}
func (rf *Raft) getFirstIdxWithTerm(term int) int {
	for i, e := range rf.log {
		// skip if first (nil) log, or if its already committed
		if i == 0 || i <= rf.commitIndex {
			continue
		}

		if e.Term == term {
			return i
		}
	}
	return -1
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAndProcessAE(follower int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.readPersist(rf.persister.ReadRaftState())
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == leaderNode
	if isLeader {
		// append command to local log
		DPrintf("[%v] receiving cmd %v-%v from client, at idx %v", rf.me, term, command, index)
		rf.log = append(rf.log, &entry{command, term})
		//DPrintf("[%v] persist: save. after receipt of log from client {l%v, T%v v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
		rf.persist()
	}
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

// Called by a leader who has at least 1 new entry that must be pushed to follower
// If the follower accepts the entries, only one AE RPC is sent
// If not, we backtrack and send until the follower accepts or until we hear from a new leader
func (rf *Raft) pushLogsToFollower(follower int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("[%v] start of push logs to foll %v", rf.me, follower)
	for !rf.killed() && rf.state == leaderNode {
		//DPrintf("[%v] start of loop in push logs to foll %v. nextIdx: %v %v %v", rf.me, follower, rf.nextIdx[0], rf.nextIdx[1], rf.nextIdx[2])
		// Send entries starting at nextIdx for this follower
		follNextIdx := rf.nextIdx[follower]
		//DPrintf("[%v] creating AE with first entry to send (to foll %v) at idx %v", rf.me, follower, follNextIdx)
		if follNextIdx < 1 {
			DPrintf("[%v] SHOULD NOT REACH exiting early from pushLogsToFollower b/c rf.nextIdx for foll %v is < 1", rf.me, follower)
			return
		}
		entriesToSend := rf.log[follNextIdx:]

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIdx:   follNextIdx - 1,
			PrevLogTerm:  rf.log[follNextIdx-1].Term,
			Entries:      entriesToSend,
			LeaderCommit: rf.commitIndex,
		}

		var reply AppendEntriesReply

		//rf.waitingOnRPC[follower] = true
		//DPrintf("[%v] setting wait on RPC for foll %v", rf.me, follower)

		//DPrintf("[%v] sending AE to foll %v: %v entries", rf.me, follower, len(args.Entries))
		//argsEntriesLenBeforeAeRpc := len(args.Entries)
		DPrintf("[%v] about to unlock for sendAE in pushLogs", rf.me)
		rf.mu.Unlock()
		DPrintf("[%v] unlocked for sendAE in pushLogs", rf.me)
		ok := rf.sendAppendEntries(follower, &args, &reply)
		rf.mu.Lock()
		//argsEntriesLenAfterAeRpc := len(args.Entries)
		//DPrintf("[%v] argsEntries Before & After sending them to follower %v in AE: before (%v) and after (%v) Are they equal? %v", rf.me, follower, argsEntriesLenBeforeAeRpc, argsEntriesLenAfterAeRpc, argsEntriesLenBeforeAeRpc == argsEntriesLenAfterAeRpc)

		if !ok {
			// next tick we'll spawn a new pushLogsToFollower(), and we want the current goroutine to have ended
			// The new goroutine will handle new logs and previous ones (those the current goroutine failed to push)
			DPrintf("[%v] attempt to send AE to foll %v failed (%v entries): !ok", rf.me, follower, len(args.Entries))
			//rf.waitingOnRPC[follower] = false
			//DPrintf("[%v] unsetting wait on RPC for foll %v due to bad rpc response", rf.me, follower)

			// TODO: Should we become follower here???
			//rf.becomeFollower()
			return
		}

		if reply.Term > args.Term {
			DPrintf("[%v] push of logs to foll %v was invalid: our term (%v) was less than follower's (%v). becoming follower", rf.me, follower, rf.currentTerm, reply.Term)
			rf.currentTerm = reply.Term
			//DPrintf("[%v] persist: save. after sending AE to %v from pushLogs results in early exit {l%v, T%v v%v}", rf.me, follower, len(rf.log), rf.currentTerm, rf.votedFor)

			rf.persist()
			rf.becomeFollower()
			//rf.waitingOnRPC[follower] = false
			//DPrintf("[%v] unsetting wait on RPC for foll %v due to invalid RPC sent (not leader anymore)", rf.me, follower)

			return
		}

		// Otherwise, if we're here and have no success, we have log inconsistency. To back up faster, decrement nextIdx by smart amount, then retry
		// 3 Cases to handle:
		/// 1. If leader doesn't have xterm; nextIdx = xIdx
		/// 2. If leader has xterm; nextIdx = idx of leader's last entry for xterm
		/// 3. follower's log is too short; nextIdx = xLen
		DPrintf("[%v] before fast back up processing of foll %v", rf.me, follower)
		if !reply.Success {
			rf.processLogInconsistency(follower, reply.XIdx, reply.XTerm, reply.XLength)
			continue
		}
		DPrintf("[%v] after fast back up processing", rf.me)

		// We have success - update nextIdx and matchIdx
		//DPrintf("[%v] updating (for foll %v) nextIdx: %v -> %v. matchIdx: %v -> %v", rf.me, follower, rf.nextIdx[follower], rf.nextIdx[follower]+len(entriesToSend), rf.matchIdx[follower], args.PrevLogIdx+len(entriesToSend))
		// Increment monotonically
		var newNextIdx int
		// On non-HBs, where we're sending the first entry,
		//if args.PrevLogIdx == 0 && len(args.Entries) > 0 {
		//	fmt.Println("REACHED")
		//	newNextIdx = max(rf.nextIdx[follower]+len(entriesToSend), 1+len(entriesToSend))
		//} else {

		// If we're sending the first log entry, and args.PrevLogIdx == 0, treat it as 1
		//newNextIdx = max(rf.nextIdx[follower]+len(entriesToSend), args.PrevLogIdx+len(entriesToSend), 1+len(entriesToSend))

		DPrintf("[%v] in pushLogs is setting nextIdx for foll %v from %v to max(%v, %v)", rf.me, follower, rf.nextIdx[follower], rf.nextIdx[follower]+len(entriesToSend), 1+len(entriesToSend))
		newNextIdx = max(rf.nextIdx[follower]+len(entriesToSend), 1+len(entriesToSend))
		//}
		DPrintf("[%v] updating nextIdx for foll %v from %v -> %v", rf.me, follower, rf.nextIdx[follower], newNextIdx)
		rf.nextIdx[follower] = newNextIdx // Can't just increment by len(entriesToSend) b/c in unreliable instance could receive responses from multiple rpc's that were sending the same args.Entries.
		rf.matchIdx[follower] = max(rf.matchIdx[follower], args.PrevLogIdx+len(entriesToSend))

		// Every time we successfully send logs from a leader to a follower, check:
		//// Should we mark more of our entries as committed?
		//// AKA are there any entries that we haven't yet marked as committed, AND are replicated on a majority of servers, AND are in the current term?
		//// loop backwards over rf.log
		for c := len(rf.log) - 1; c > 0 && !rf.killed(); c-- {
			// Entry at c is only a candidate for commit if term is currentterm
			if rf.log[c].Term == rf.currentTerm {
				//DPrintf("[%v] c (%v) in current Term: determining whether to update rf.commitIdx from %v to %v (c)?", rf.me, c, rf.commitIndex, c)
				if rf.isNotYetCommitted(c) && rf.isReplicatedOnMajority(c) && rf.isInSameTerm(c) {
					DPrintf("[%v] updates commitIdx to c %v after push to %v", rf.me, c, follower)
					rf.commitIndex = c
					rf.attemptApply()
					break
				}
			}
		}
		//rf.waitingOnRPC[follower] = false
		//DPrintf("[%v] unsetting wait on RPC for foll %v due to successful push", rf.me, follower)
		break
	}
}

func (rf *Raft) processLogInconsistency(follower int, XIdx int, XTerm int, XLength int) {
	leaderHasXTerm, leadersLastEntryForXTerm := rf.lookupXTerm(XTerm)

	if XTerm == -1 { // Case 3
		DPrintf("[%v] fast back up case 3 (follower's log len is too short). nextIdx for foll %v: %v -> %v (xLength)", rf.me, follower, rf.nextIdx[follower], XLength)

		rf.nextIdx[follower] = XLength
	} else if !leaderHasXTerm { // Case 1
		DPrintf("[%v] fast back up case 1 (leader doesn't have xTerm %v). nextIdx for foll %v: %v -> %v (xIdx)", rf.me, XTerm, follower, rf.nextIdx[follower], XIdx)
		rf.nextIdx[follower] = XIdx
	} else { // Case 2
		DPrintf("[%v] fast back up case 2 (leader has xTerm %v). nextIdx for foll %v: %v -> %v (leadersLastEntryForXTerm)", rf.me, XTerm, follower, rf.nextIdx[follower], leadersLastEntryForXTerm) // TODO: are we getting here??
		rf.nextIdx[follower] = leadersLastEntryForXTerm + 1
	}
}

// Returns bool (does rf have any entries with follXTerm?) and it's idx if so, -1 if not
func (rf *Raft) lookupXTerm(follXTerm int) (hasXTerm bool, leaderXTermIdx int) {
	// loop backwards over leader (this) log
	// search for entry with term == follXTerm
	// if found, return true, that first entry's idx
	// if not found, return false, -1
	for i := len(rf.log) - 1; i > 0; i-- {
		// TODO: WE shouldn't skip if we've already been committed - should we skip if we haven't been committed?
		//if i <= rf.commitIndex {
		//	continue
		//}
		leaderEntry := rf.log[i]
		if leaderEntry.Term == follXTerm {
			DPrintf("[%v] (leader) found an entry at %v with follower's conflicting term %v", rf.me, i, follXTerm)
			return true, i
		}
	}
	return false, -1
}

// Following 3 functions:
// Called by leader, when lock held
func (rf *Raft) isNotYetCommitted(logIdx int) bool {
	ret := logIdx > rf.commitIndex
	if !ret {
		//DPrintf("[%v]. Should we commit log idx %v? failed: %v already been committed (rf.commitIdx: %v)", rf.me, logIdx, logIdx, rf.commitIndex)
	}
	return ret
}
func (rf *Raft) isReplicatedOnMajority(logIdx int) bool {
	majority := len(rf.peers)/2 + 1
	count := 0

	for p := 0; p < len(rf.peers); p++ {
		// increment count if: we're looping over self OR this entry is marked as replicated
		if p == rf.me || (len(rf.matchIdx) > p && rf.matchIdx[p] >= logIdx) {
			count++
		}
	}

	ret := count >= majority
	if !ret {
		//DPrintf("[%v]. Should we commit log idx %v? failed: idx %v only replicated on %v ([%v] does not yet recognize majority (%v))", rf.me, logIdx, logIdx, count, rf.me, majority)
	}
	return ret
}
func (rf *Raft) isInSameTerm(logIdx int) bool {
	ret := rf.log[logIdx].Term == rf.currentTerm
	if !ret {
		//DPrintf("[%v]. Should we commit log idx %v? failed: term of %v (T%v) is different from ours: T%v", rf.me, logIdx, logIdx, rf.log[logIdx].Term, rf.currentTerm)
	}
	return ret
}

//func (rf *Raft) apply() {
//	for !rf.killed() {
//		time.Sleep(time.Duration(40) * time.Millisecond)
//
//		rf.mu.Lock()
//
//		// if commitIdx > lastApplied, send to apply channel
//		if rf.commitIndex > rf.lastApplied {
//			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
//				//DPrintf("[%v] sending applyMsg idx %v. commitIdx (%v) > lastApplied (%v)", rf.me, i, rf.commitIndex, rf.lastApplied)
//				rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Cmd, CommandIndex: i}
//				rf.lastApplied++
//			}
//		}
//		rf.mu.Unlock()
//	}
//}

// A ticker function that handlers the election process
func (rf *Raft) ticker() {

	// Exit when killed
	for !rf.killed() {
		tickInterval := 50 * time.Millisecond // 50???

		rf.mu.Lock()

		switch rf.state {
		case followerNode:
			// if a follower has not received a HB recently, go directly to candidate state

			if time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout {
				rf.state = candidateNode
				//DPrintf("[%v] (foll) heartbeatTimeout occurred. becoming %v cand, starting election", rf.me, rf.currentTerm+1)
				rf.becomeCandidate()
				//rf.mu.Unlock()
				//go rf.startElection() // kick off election immediately, for first time
				rf.broadcastVotes()
				rf.mu.Unlock()
				continue
			}
			//fallthrough
		case candidateNode:
			// First time around, we'll get here after becoming cand. and immediately coming here. !rf.recHBRec will still be true, so go startElec.
			// Then, wait. Hopefully, while we're waiting, we'll vote for self & receive HB. We could still be cand, but no new election spawned. Wait again, maybe by next time around we'll win and be leader
			// But maybe network failure, we won't vote for self so no HB received. we'll still be cand - go start another election.
			//if !rf.recentHeartbeatReceived {

			// Now, we've already started an election (from starting in follower state), but check to see if its gone on too long
			if time.Since(rf.electionStartedAt) > rf.electionTimeout {
				//DPrintf("[%v] (cand) electionTimeout occurred. becoming cand, restarting election", rf.me)
				rf.becomeCandidate()
				rf.broadcastVotes()
				//go rf.startElection()
				//} else {
				//	// if we have received a recent HB, reset it, proceed to wait again
				//	rf.recentHeartbeatReceived = false
				//rf.lastHeartbeat = time.Now()
			}
		case leaderNode:
			//if time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout {
			//	DPrintf("[%v] resetting heartbeat timer as its been a while", rf.me)
			//	rf.lastHeartbeat = time.Now()
			//}
			// check our log: do we see a new entry? (aka is leader's lastLogIdx >= follower's nextIdx?) if so, startAgreement. Otherwise, send HBs to everyone.

			// Loop through followers. Push new entries via startAgreement OR send HB in this idle period. For self, ensure matchIdx is correct
			for i := 0; i < len(rf.peers); i++ {
				//DPrintf("[%v] for peer %v, nextIdx is: %v", rf.me, i, rf.nextIdx[i])
				if i == rf.me {
					// don't push logs to self, but we do need to update matchIdx for self
					//DPrintf("[%v] updating matchIdx for foll (self) to: %v -> %v (commitIdx)", rf.me, rf.matchIdx[rf.me], rf.commitIndex)
					rf.matchIdx[rf.me] = rf.commitIndex

					// Usually, we notify nodes of HB on pushLogs or sendHB, but since we'll skip that for ourself, tell ourself to record a HB
					//rf.lastHeartbeat = time.Now()

					// if our log's last len is greater than the nextIdx
				} else if len(rf.log) > rf.nextIdx[i] {
					//if !rf.waitingOnRPC[i] { // don't spawn new pushLogs if still waiting for the last one
					// If we have a new log entry that must be pushed to follower i, spawn agreement process
					DPrintf("[%v] will push %v entries to foll %v as nextIdx[i] is: %v and len(rf.log) is %v", rf.me, len(rf.log[rf.nextIdx[i]:]), i, rf.nextIdx[i], len(rf.log))
					go rf.pushLogsToFollower(i)
					//} else {
					//	DPrintf("[%v] not respawning pushLogs goroutine", rf.me) // todo: problematic when nodes are disconnected? or good?
					//}
				} else {
					// Not necessary to send HBs to self b/c we only check for !recentHBReceived if we're a follower or cand
					DPrintf("[%v] sending HB to foll %v", rf.me, i)
					go rf.sendHeartbeatToNode(i)
				}
			}
		}
		rf.mu.Unlock()
		//DPrintf("[%v] beginning sleep in ticker", rf.me)
		time.Sleep(tickInterval)
		//DPrintf("[%v] done with sleep in ticker", rf.me)
	}
}

// Call when rf.mu is locked
func (rf *Raft) broadcastVotes() {
	for i := 0; i < len(rf.peers); i++ {
		if rf.state == candidateNode {
			if i != rf.me {
				lastLogIdx := len(rf.log) - 1
				args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIdx, rf.log[lastLogIdx].Term}
				var reply RequestVoteReply
				go rf.sendRequestVote(i, &args, &reply)
			}
		}
	}
}

func (rf *Raft) becomeFollower() {
	rf.state = followerNode
	rf.lastHeartbeat = time.Now()
}

// Call when rf.mu is locked
func (rf *Raft) becomeCandidate() {
	rf.currentTerm++
	rf.votesCollected = make([]bool, len(rf.peers))
	rf.votesCollected[rf.me] = true
	rf.votedFor = rf.me
	rf.resetElectionTimeout()
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	DPrintf("[%v] wins T%v, becoming leader", rf.me, rf.currentTerm)
	rf.state = leaderNode
	initialNextIdx := len(rf.log)
	DPrintf("[%v] nextIdx for all follower init'ed to: %v", rf.me, initialNextIdx)
	rf.nextIdx = make([]int, 0, len(rf.peers))
	rf.matchIdx = make([]int, 0, len(rf.peers))
	//rf.waitingOnRPC = make([]bool, 0, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIdx = append(rf.nextIdx, initialNextIdx)
		rf.matchIdx = append(rf.matchIdx, 0)
		//rf.waitingOnRPC = append(rf.waitingOnRPC, false)
	}
}

// send 1 HB to nodeIdx & process response
func (rf *Raft) sendHeartbeatToNode(nodeIdx int) {
	rf.mu.Lock()
	var reply AppendEntriesReply
	// When thinking about PrevLog in a HB - we have 0 entries to apply to follower, so the previous entry is just the last one in this leader's log
	// Important for determining whether to overwrite
	//args := AppendEntriesArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term, make([]*entry, 0), rf.commitIndex}
	args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIdx[nodeIdx] - 1, rf.log[len(rf.log)-1].Term, make([]*entry, 0), rf.commitIndex}

	if rf.state != leaderNode {
		rf.mu.Unlock()
		DPrintf("[%v] was going to send HB to %v, but is no longer leader", rf.me, nodeIdx)
		return
	}

	rf.mu.Unlock()

	rpcOk := rf.sendAppendEntries(nodeIdx, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rpcOk {
		// on network failure, downgrade leader to follower

		// We could get !ok because we are disconnected. In which case, downgrade to follower.
		// We could also get !ok because follower (nodeIdx) is disconnected. Should we remain leader in that case
		DPrintf("[%v] received !ok response to AE on send of HB to foll %v - returning", rf.me, nodeIdx)
		//rf.becomeFollower()
		// say we remain leader. we'll return to ticker, and attempt to send HB to any remaining followers.
		// what if we're disconnected? we'll remain leader, other nodes will move on. When we connect, we'll figure it out
		// What if the follower is disconnected? We'll remain leader, sending HBs to other (connected) followers
		// This sounds good - don't downgrade on network fail
	} else if reply.Term > args.Term {
		rf.currentTerm = reply.Term
		DPrintf("[%v] received not success response to AE on send of HB to foll %v (foll's term > ours). becoming follower", rf.me, nodeIdx)
		rf.becomeFollower()
	} else if !reply.Success {
		// Get to here if there's a log inconsistency
		DPrintf("[%v] (leader) processing log incons. with %v (follower) after HB sent", rf.me, nodeIdx)
		rf.processLogInconsistency(nodeIdx, reply.XIdx, reply.XTerm, reply.XLength)
	}
	//DPrintf("[%v] persist: save after sending HB {l%v, T%v v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
	rf.persist()
}

func (rf *Raft) resetElectionTimeout() {
	electionTimout := 300 + (rand.Int63() % 300)
	rf.electionTimeout = time.Duration(electionTimout) * time.Millisecond
	rf.electionStartedAt = time.Now()
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
	rf.log = append(rf.log, &entry{nil, 0}) // We want entries to be zero-indexed
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.votesCollected = make([]bool, len(rf.peers))
	rf.heartbeatTimeout = 150 * time.Millisecond // Tested from 50 - 250, 150 seemed best
	//DPrintf("[%v] initial reset of election timer", rf.me)
	rf.electionTimeout = time.Duration(300+(rand.Int63()%300)) * time.Millisecond
	rf.lastHeartbeat = time.Now()

	// initialize from state persisted before a crash
	//DPrintf("[%v] persist: load during Make", rf.me)

	rf.readPersist(persister.ReadRaftState())

	// start periodic check for committed logs so they can be applied (this will be more frequent than ticker)
	//go rf.commit()

	// start ticker goroutine to start elections
	go rf.ticker()
	//go rf.applyPeriodically()

	return rf
}
