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
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Volatile state for each server
	state                   serverType // needs locking
	recentHeartbeatReceived bool       // needs locking
	commitIndex             int        // index of highest log entry known to be committed // needs locking
	lastApplied             int        // index of highest log entry executed // needs locking

	// Volatile state applicable to leaders
	nextIdx  []int // for each server (or follower?), idx of next log entry to send to it
	matchIdx []int // for each server (follower?), idx of highest log entry known to be replicated on server

	// TODO (3C) Must be updated into persister before responding to RPCs
	currentTerm int      // latest term this server has seen // needs locking
	votedFor    int      // candidateId that received vote of this server in current term (or -1 if none)
	log         []*entry // needs locking

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
	// Your code here (3C).
	// Example:
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
	// Your code here (3C).
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

	// We'll read from currTerm, votedFor, and log. If we just restarted, these must be reloaded
	//DPrintf("[%v] persist: load. at beginning of RV (from %v) rpc handler", rf.me, args.CandidateId)
	rf.readPersist(rf.persister.ReadRaftState())

	// First, handle invalid RequestVote RPC
	// Invalid if cand's term is lower than ours
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// Otherwise, candidate (aka RPC requester)'s term is >= ours

	// if requester's term is strictly higher, reset votedFor
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.state = followerNode
	}

	// Update our term, set reply.Term to it
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	// Next, vote as appropriate
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandLogAtLeastAsUpToDateAsVoters(args.LastLogTerm, args.LastLogIdx) {
		// record HB iff we vote
		rf.recentHeartbeatReceived = true

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true

		// If we're voting yes for another (not self) node, downgrade self to follower
		if rf.me != args.CandidateId {
			rf.state = followerNode
		}
	}

	//DPrintf("[%v] persist: save. at end of RV RPC handler {l%v, T%v v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
	rf.persist()

}

// For a yes vote, the candidate's log must also be at least as up to date as the voters
// called with lock held
func (rf *Raft) isCandLogAtLeastAsUpToDateAsVoters(lastLogTermCand int, lastLogIdxCand int) bool {
	// When comparing two logs, up-to-date is defined as:
	// if the last entries of each log have different terms, then the log w a later term is more up to date
	// if they have the same term, the longer log is more up to date

	var ret bool
	lastLogTermMe := rf.log[len(rf.log)-1].Term

	if lastLogTermCand != lastLogTermMe {
		ret = lastLogTermCand >= lastLogTermMe
	} else {
		ret = lastLogIdxCand+1 >= len(rf.log)
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
	return ok
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

	// Regardless of what happens, record heartbeat
	rf.recentHeartbeatReceived = true

	// We may have just restarted - read persistent state
	//DPrintf("[%v] persist: load at beginning of AE (from %v) rpc handler", rf.me, args.LeaderId)
	rf.readPersist(rf.persister.ReadRaftState())

	// First, handle invalid AppendEntries RPC
	// if sender's term is less than receiver's, fail and set reply.Term = the higher receiver's
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// Early exit if leader's prevLogIdx is out of range of our log - this matches Case 3 of fast backup
	if args.PrevLogIdx < 0 || args.PrevLogIdx >= len(rf.log) {
		DPrintf("[%v], failure on attempt to append %v entries, has prevLogIdx %v out of range. len(rf.log): %v. our log: %v", rf.me, len(args.Entries), args.PrevLogIdx, len(rf.log), printEntries(rf.log, 0))
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XLength = len(rf.log)
		reply.XTerm = -1
		reply.XIdx = -1
		return
	}

	// Early exit - Conflicting terms at leader's PrevLogIdx, match for Cases 1,2 of fast backup
	if rf.log[args.PrevLogIdx].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = rf.log[args.PrevLogIdx].Term
		reply.XIdx = rf.getFirstIdxWithTerm(rf.log[args.PrevLogIdx].Term)

		//reply.XIdx = rf.getLatestIdxWithTerm(rf.log[args.PrevLogIdx].Term)
		DPrintf("[%v], found conflicting entries when appending %v entries. Our term at prevLogIdx (%v): %v != PrevLogTerm: %v. Set XIdx: %v", rf.me, len(args.Entries), args.PrevLogIdx, rf.log[args.PrevLogIdx].Term, args.PrevLogTerm, reply.XIdx)
		DPrintf("[%v] failure on attempt to accept leader %v's push of entries: %v", rf.me, args.LeaderId, printEntries(args.Entries, 0))
		return
	}

	// Otherwise, RPC requester is seen as leader, as it's term is >= ours
	// Update our term, set reply.Term to it and make ourself a follower
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	if rf.me != args.LeaderId { // in theory we should not need to handle this call from self, but check just in case
		// Unless we're sending to ourself (in which case we'd like to remain the leader), update our state to follower
		rf.state = followerNode
	}

	// At this point, if we were going to reject the new entries, we would have already
	reply.Success = true

	// Follower must remove any entries in our log which conflict with those of the leader
	// range over args.Entries (leader's log) with leaderIdx, leaderEntry
	// if follEntry's term != leader's entry's term, delete it and those following

	DPrintf("[%v] attempt at receiving %v entries from leader %v: args.PrevLogIdx: %v. len(rf.log): %v", rf.me, len(args.Entries), args.LeaderId, args.PrevLogIdx, len(rf.log))

	if args.PrevLogIdx < len(rf.log) {
		for leaderIdx, leaderEntry := range args.Entries {
			follIdx := 1 + args.PrevLogIdx + leaderIdx
			if follIdx >= len(rf.log) {
				break
			}
			follEntry := rf.log[follIdx]
			if follEntry.Term != leaderEntry.Term {
				// found conflicting entry
				DPrintf("[%v] deleting entry (idx %v) from log and all following: conflicting terms: follEntry.Term: %v != %v : leaderEntry.Term", rf.me, follIdx, follEntry.Term, leaderEntry.Term)
				// delete it and those following
				// first, don't leak memory (necessary since log entries contain pointers)
				for j := follIdx; j < len(rf.log); j++ {
					rf.log[j] = nil
				}

				// then, re-assign rf.log, shortening its length
				rf.log = rf.log[0:follIdx]
			}
		}
	}

	// We've also deleted any entries in preparation for overwriting
	// Append any new entries not already in the log
	// Either there was no overlap (append at end of rf.log) OR there was overlap and we deleted any entries with conflicting terms.
	// Naturally skip on HBs
	for i := 0; i < len(args.Entries); i++ {
		DPrintf("[%v] received cmd %v-%v from leader %v. Adding to log at idx %v", rf.me, args.Entries[i].Term, args.Entries[i].Cmd, args.LeaderId, len(rf.log))
		// If our commitIdx is higher than args.PrevLogIdx, then skip
		if rf.commitIndex > args.PrevLogIdx {
			continue
		}
		rf.log = append(rf.log, args.Entries[i])
	}

	if len(args.Entries) > 0 {
		DPrintf("[%v] success: %v entries added to log. new len: %v", rf.me, len(args.Entries), len(rf.log))
	}
	// Since our log is now equivalent to the leaders
	// Learn from leader which entries in our newly updated log have been committed
	// Do this on actual AE's && HB's
	DPrintf("[%v] during receipt of %v entries from leader %v, check commitIdx: args.LeaderCommit: %v: rf.commitIdx: %v", rf.me, len(args.Entries), args.LeaderId, args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("[%v] updating commitIdx from %v to min(%v, %v)", rf.me, rf.commitIndex, args.LeaderCommit, len(rf.log)-1)
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	// With normal exit, we've edited rf.log, rf.currentTerm (must remain persistent). Save before returning
	//DPrintf("[%v] persist: save. at end of AE RPC handler {l%v, T%v v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
	rf.persist()
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

func (rf *Raft) getLatestIdxWithTerm(term int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		e := rf.log[i]
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
	//DPrintf("[%v] persist: load. before attempting to accept %v from client", rf.me, command)
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
	//DPrintf("[%v] persist: load. at beginning of pushLogs (to %v) func rpc handler", rf.me, follower)

	rf.readPersist(rf.persister.ReadSnapshot())
	for !rf.killed() && rf.state == leaderNode {
		// Send entries starting at nextIdx for this follower
		firstEntryToSend := rf.nextIdx[follower]
		if firstEntryToSend < 1 {
			break
		}
		entriesToSend := rf.log[firstEntryToSend:]

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIdx:   firstEntryToSend - 1,
			PrevLogTerm:  rf.log[firstEntryToSend-1].Term,
			Entries:      entriesToSend,
			LeaderCommit: rf.commitIndex,
		}

		var reply AppendEntriesReply

		rf.mu.Unlock()
		ok := rf.sendAppendEntries(follower, &args, &reply)
		rf.mu.Lock()

		if !ok {
			// next tick we'll spawn a new pushLogsToFollower(), and we want the current goroutine to have ended
			// The new goroutine will handle new logs and previous ones (those the current goroutine failed to push)
			DPrintf("[%v] attempt to send AE to foll %v failed (%v entries): !ok", rf.me, follower, len(args.Entries))

			// update nextIdx for follower, so we'll send them again
			//rf.nextIdx[follower] -= len(args.Entries)
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			//DPrintf("[%v] persist: save. after sending AE to %v from pushLogs results in early exit {l%v, T%v v%v}", rf.me, follower, len(rf.log), rf.currentTerm, rf.votedFor)

			rf.persist()
			rf.state = followerNode
			return
		}

		// Otherwise, if we're here and have no success, we have log inconsistency. To back up faster, decrement nextIdx by smart amount, then retry
		// 3 Cases to handle:
		/// 1. If leader doesn't have xterm; nextIdx = xIdx
		/// 2. If leader has xterm; nextIdx = idx of leader's last entry for xterm
		/// 3. follower's log is too short; nextIdx = xLen
		if !reply.Success {
			leaderHasXTerm, leadersLastEntryForXTerm := rf.lookupXTerm(reply.XTerm)

			if reply.XTerm == -1 { // Case 3
				DPrintf("[%v] fast back up case 3 (follower's log len is too short). nextIdx for foll %v: %v -> %v (xLength)", rf.me, follower, rf.nextIdx[follower], reply.XLength)

				rf.nextIdx[follower] = reply.XLength
			} else if !leaderHasXTerm { // Case 1
				DPrintf("[%v] fast back up case 1 (leader doesn't have xTerm %v). nextIdx for foll %v: %v -> %v (xIdx)", rf.me, reply.XTerm, follower, rf.nextIdx[follower], reply.XIdx)
				rf.nextIdx[follower] = reply.XIdx
			} else { // Case 2
				DPrintf("[%v] fast back up case 2 (leader has xTerm %v). nextIdx for foll %v: %v -> %v (leadersLastEntryForXTerm)", rf.me, reply.XTerm, follower, rf.nextIdx[follower], leadersLastEntryForXTerm)
				rf.nextIdx[follower] = leadersLastEntryForXTerm
			}
			continue
		}

		// We have success - update nextIdx and matchIdx
		DPrintf("[%v] updating (for foll %v) nextIdx: %v -> %v. matchIdx: %v -> %v", rf.me, follower, rf.nextIdx[follower], rf.nextIdx[follower]+len(entriesToSend), rf.matchIdx[follower], args.PrevLogIdx+len(entriesToSend))
		rf.nextIdx[follower] += len(entriesToSend)
		rf.matchIdx[follower] = args.PrevLogIdx + len(entriesToSend)

		// Every time we successfully send logs from a leader to a follower, check:
		//// Should we mark more of our entries as committed?
		//// AKA are there any entries that we haven't yet marked as committed, AND are replicated on a majority of servers, AND are in the current term?
		//// loop backwards over rf.log
		for c := len(rf.log) - 1; c > 0; c-- {
			// Entry at c is only a candidate for commit if term is currentterm
			if rf.log[c].Term == rf.currentTerm {
				DPrintf("[%v] c (%v) in current Term: determining whether to update rf.commitIdx from %v to %v (c)?", rf.me, c, rf.commitIndex, c)
				if rf.isNotYetCommitted(c) && rf.isReplicatedOnMajority(c) && rf.isInSameTerm(c) {
					DPrintf("[%v] updates commitIdx to c %v", rf.me, c)
					rf.commitIndex = c
					break
				}
			} else {
			}
		}
		break
	}
}

// Returns bool (does rf have any entries with follXTerm?) and it's idx if so, -1 if not
func (rf *Raft) lookupXTerm(follXTerm int) (hasXTerm bool, leaderXTermIdx int) {
	// loop backwards over leader (this) log
	// search for entry with term == follXTerm
	// if found, return true, that first entry's idx
	// if not found, return false, -1
	for i := len(rf.log) - 1; i > 0; i-- {
		if i <= rf.commitIndex {
			continue
		}
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
		DPrintf("[%v]. Should we commit log idx %v? failed: %v already been committed (rf.commitIdx: %v)", rf.me, logIdx, logIdx, rf.commitIndex)
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
		DPrintf("[%v]. Should we commit log idx %v? failed: idx %v only replicated on %v ([%v] does not yet recognize majority (%v))", rf.me, logIdx, logIdx, count, rf.me, majority)
	}
	return ret
}
func (rf *Raft) isInSameTerm(logIdx int) bool {
	ret := rf.log[logIdx].Term == rf.currentTerm
	if !ret {
		DPrintf("[%v]. Should we commit log idx %v? failed: term of %v (T%v) is different from ours: T%v", rf.me, logIdx, logIdx, rf.log[logIdx].Term, rf.currentTerm)
	}
	return ret
}

func (rf *Raft) apply() {
	for !rf.killed() {
		time.Sleep(time.Duration(20) * time.Millisecond)

		rf.mu.Lock()

		// if commitIdx > lastApplied, send to apply channel
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				DPrintf("[%v] sending applyMsg idx %v. commitIdx (%v) > lastApplied (%v)", rf.me, i, rf.commitIndex, rf.lastApplied)
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Cmd, CommandIndex: i}
				rf.lastApplied++
			}
		}
		rf.mu.Unlock()
	}
}

// A ticker function that handlers the election process
func (rf *Raft) ticker() {

	// Exit when killed
	for !rf.killed() {
		// Choose new electionTimeout interval each period so an unlucky set of choices don't haunt us for whole life of node
		// electionTimeout will be between 200 & 600 ms
		timeoutInterval := 200 + (rand.Int63() % 400)

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
			// check our log: do we see a new entry? (aka is leader's lastLogIdx >= follower's nextIdx?) if so, startAgreement. Otherwise, send HBs to everyone.

			// Loop through followers. Push new entries via startAgreement OR send HB in this idle period. For self, ensure matchIdx is correct
			for i := 0; i < len(rf.peers); i++ {

				if i == rf.me {
					// don't push logs to self, but we do need to update matchIdx for self
					DPrintf("[%v] updating matchIdx for foll (self) to: %v -> %v (commitIdx)", rf.me, rf.matchIdx[rf.me], rf.commitIndex)
					rf.matchIdx[rf.me] = rf.commitIndex

					// Usually, we notify nodes of HB on pushLogs or sendHB, but since we'll skip that for ourself, tell ourself to record a HB
					rf.recentHeartbeatReceived = true
				} else if len(rf.log)-1 >= rf.nextIdx[i] {
					// If we have a new log entry that must be pushed to follower i, spawn agreement process
					//DPrintf("[%v] will push %v entries to foll %v", rf.me, len(rf.log[rf.nextIdx[i]:]), i)
					go rf.pushLogsToFollower(i)
				} else {
					// Not necessary to send HBs to self b/c we only check for !recentHBReceived if we're a follower or cand
					go rf.sendHeartbeatToNode(i)
				}
			}
		}
		rf.mu.Unlock()
	}
}

// called by a candidate rf server. lock not held
func (rf *Raft) startElection() {
	rf.mu.Lock()
	//DPrintf("[%v] persist: load at beginning of startElection func", rf.me)

	rf.readPersist(rf.persister.ReadSnapshot())
	rf.currentTerm++
	//DPrintf("[%v] starting election, T%v", rf.me, rf.currentTerm)

	// About to communicate our info to outside world - make sure we'll remember it ourselves if we crash
	//DPrintf("[%v] persist: save. after updating currentTerm in startElection {l%v, T%v v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
	rf.persist()

	rf.recentHeartbeatReceived = false
	rf.mu.Unlock()

	yesVotes := 0
	voteCount := 0
	var voteMutex sync.Mutex
	cond := sync.NewCond(&voteMutex)

	// Spawn len(rf.peers) goroutines, that each request a vote from a different node
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.mu.Lock()
			rf.votedFor = rf.me
			//DPrintf("[%v] persist: save. after voting for self in startElection {l%v, T%v v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
			rf.persist()
			rf.mu.Unlock()
			yesVotes++
		} else {
			go func(nodeIdx int) {
				// Loop till we make a successful RPC requestVote call and get either a yes or no vote
				for !rf.killed() {

					rf.mu.Lock()
					// Don't RV if an earlier peer demoted self
					if rf.state != followerNode {

						var reply RequestVoteReply
						lastLogIdx := len(rf.log) - 1
						args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIdx, rf.log[lastLogIdx].Term}

						// TODO (remove?) do i need to readPersist here? No need to call readPersist here. Need to read if we're worried about getting here after a crash, and having blank currentTerm, etc. After a crash, we won't resume in the middle of this goroutine (it'll have crashed too). The next resume, if any in this function, will be another call to startElection

						// Do not hold lock when sending RPC
						rf.mu.Unlock()

						rpcOk := rf.sendRequestVote(nodeIdx, &args, &reply)

						voteMutex.Lock()
						rf.mu.Lock()

						// TODO: (remove?) need to call readPersist here? Say we requested a vote from a peer. While the peer is executing its RV handler, we crash. We won't resume here, but with another call to startElection, then another call to sendRV, so NO

						// 2 outcomes for RPC call:
						// 1. success, receive either yes or no vote
						// 2. RPC did not go through. If this is because follower is down, we should retry later so it has a chance to come back up

						if rpcOk {
							// if RPC recipient's term is higher than this candidate's term, we'll have to update cand (soon to be follower, since vote will not have been granted)'s term
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								//DPrintf("[%v] persist: save after receiving RV rpc response, updating rf.currentTerm {l%v, T%v v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
								rf.persist()
								rf.state = followerNode
							} else if reply.VoteGranted {
								yesVotes++
							} else {
								rf.state = followerNode
							}
						}
					} else {
						voteMutex.Lock()
					}
					// Regardless of whether we sent the RV, and if we got yes/no vote, we must ++ voteCount & broadcast, so we'll know when to tally.
					voteCount++
					cond.Broadcast()
					rf.mu.Unlock()
					voteMutex.Unlock()
					return
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
		DPrintf("[%v] wins T%v", rf.me, rf.currentTerm)
		rf.state = leaderNode
		// Initialize nextIdx for all followers (and self), to: leader last log index + 1
		// Use tempMatchIdx, which we received from each reply, to init matchIdx for each peer
		initialNextIdx := len(rf.log)
		rf.nextIdx = make([]int, 0, 5)
		rf.matchIdx = make([]int, 0, 5)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIdx = append(rf.nextIdx, initialNextIdx)
			DPrintf("[%v] nextIdx for [%v]: %v", rf.me, i, initialNextIdx)
			rf.matchIdx = append(rf.matchIdx, 0)
		}
		DPrintf("[%v] matchIdx for [all]: inited to 0", rf.me)
		rf.mu.Unlock()
	}

	voteMutex.Unlock()
}

// send 1 HB to nodeIdx & process response
func (rf *Raft) sendHeartbeatToNode(nodeIdx int) {
	rf.mu.Lock()
	//DPrintf("[%v] persist: load. at beginning of send HB (to %v) func rpc handler", rf.me, nodeIdx)
	rf.readPersist(rf.persister.ReadRaftState())
	var reply AppendEntriesReply
	// When thinking about PrevLog in a HB - we have 0 entries to apply to follower, so the previous entry is just the last one in this leader's log
	// Important for determining whether to overwrite
	args := AppendEntriesArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term, make([]*entry, 0), rf.commitIndex}

	rf.mu.Unlock()

	rpcOk := rf.sendAppendEntries(nodeIdx, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rpcOk {
		// on network failure, downgrade leader to follower
		rf.state = followerNode
	} else if !reply.Success {
		// RPC goes through but reply.Success == fail
		// -> sender's term was less than receiver's
		// update sender's current term, downgrade to follower
		rf.currentTerm = reply.Term
		rf.state = followerNode
	}
	//DPrintf("[%v] persist: save after sending HB {l%v, T%v v%v}", rf.me, len(rf.log), rf.currentTerm, rf.votedFor)
	rf.persist()
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

	// initialize from state persisted before a crash
	//DPrintf("[%v] persist: load during Make", rf.me)

	rf.readPersist(persister.ReadRaftState())

	// start periodic check for committed logs so they can be applied (this will be more frequent than ticker)
	go rf.apply()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
