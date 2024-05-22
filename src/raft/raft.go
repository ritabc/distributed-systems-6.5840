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
	commitIndex             int        // index of highest log entry known to be committed // TODO: needs locking?
	lastApplied             int        // index of highest log entry executed // TODO: needs locking?

	// Volatile state applicable to leaders
	nextIdx  []int // for each server (or follower?), idx of next log entry to send to it
	matchIdx []int // for each server (follower?), idx of highest log entry known to be replicated on server

	// TODO (3C) Must be updated into persister before responding to RPCs
	currentTerm int      // latest term this server has seen // needs locking
	votedFor    int      // candidateId that received vote of this server in current term (or -1 if none)
	log         []*entry // needs locking?

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
	LastLogIdx  int // idx of candidate's last log entry
	LastLogTerm int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	LastLogIdx  int // index of last entry in voter's log
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

	// First, handle invalid RequestVote RPC
	// Invalid if cand's term is lower than ours
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		reply.LastLogIdx = rf.lastLogIdx()
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
		reply.LastLogIdx = rf.lastLogIdx()
	}
}

// For a yes vote, the candidate's log must also be at least as up to date as the voters
// called with lock held
func (rf *Raft) isCandLogAtLeastAsUpToDateAsVoters(lastLogTermCand int, lastLogIdxCand int) bool {
	// When comparing two logs, up-to-date is defined as:
	// if the last entries of each log have different terms, then the log w a later term is more up to date
	// if they have the same term, the longer log is more up to date

	var ret bool
	lastLogTermMe := rf.log[rf.lastLogIdx()].Term

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
	// Your code for 3B here
}

// Handler for recipient of an AppendEntries RPC call
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Regardless of what happens, record heartbeat
	rf.recentHeartbeatReceived = true

	// First, handle invalid AppendEntries RPC
	// if sender's term is less than receiver's, fail and set reply.Term = the higher receiver's
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// Fail if log doesn't contain an entry at prevLogIdx with term == prevLogTerm
	// First - do we have an entry at prevLogIdx? and
	// Second - is its term == prevLogTerm?
	// Logic:
	// if noEntryAtPrevLogIdx || itsTermIsNot==PrevLogTerm
	if args.PrevLogIdx < 0 || args.PrevLogIdx >= len(rf.log) || rf.log[args.PrevLogIdx].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
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
	// Entries from our log that are committed will not be in conflict
	// (either they would not have been committed OR the current leader wouldn't have been elected (voted for by majority))
	// In our log, loop from commitIndex+1 -> the end
	if args.PrevLogIdx < len(rf.log)-1 {
		for follIdx := rf.commitIndex + 1; follIdx < len(rf.log); follIdx++ {
			entriesFromLeaderIdx := follIdx - args.PrevLogIdx - 1
			follEntry := rf.log[follIdx]

			// follIdx has surpassed leader's entries (or it is negative)
			if entriesFromLeaderIdx < 0 || entriesFromLeaderIdx >= len(args.Entries) {
				break
			}

			entriesFromLeaderEntry := args.Entries[entriesFromLeaderIdx]

			if follEntry.Term != entriesFromLeaderEntry.Term {
				// found conflicting entry
				// delete it and those following
				// first, don't leak memory (necessary since log entries contain pointers)
				for j := follIdx; j < len(rf.log); j++ {
					rf.log[j] = nil
				}
				// then, re-assign rf.log, shortening its length
				rf.log = rf.log[0:follIdx]
				break
			}

		}
	}

	// We've also deleted any entries in preparation for overwriting
	// Append any new entries not already in the log
	// Either there was no overlap (append at end of rf.log) OR there was overlap and we deleted any entries with conflicting terms.
	// Naturally skip on HBs
	for i := 0; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}

	// Since our log is now equivalent to the leaders
	// Learn from leader which entries in our newly updated log have been committed
	// Do this on actual AE's && HB's
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIdx())
	}
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
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == leaderNode
	if isLeader {
		// append command to local log
		rf.log = append(rf.log, &entry{command, term})
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
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = followerNode
			return
		}

		if !reply.Success {
			// If we fail due to log inconsistency (reply.Success == false), decrement nextIdx and retry
			rf.nextIdx[follower]--
			continue
		}

		// On success, update nextIdx and matchIdx
		if reply.Success {
			rf.nextIdx[follower] += len(entriesToSend)
			rf.matchIdx[follower] += len(entriesToSend)

			// Every time we successfully send logs from a leader to a follower, check:
			// Should we mark more of our entries as committed?
			// AKA are there any entries that we haven't yet marked as committed, AND are replicated on a majority of servers, AND are in the current term?
			// loop backwards over rf.log
			for c := len(rf.log) - 1; c > 0; c-- {
				if rf.isNotYetCommitted(c) && rf.isReplicatedOnMajority(c) && rf.isInSameTerm(c) {
					rf.commitIndex = c
					break
				}
			}
			break
		}

	}
}

// Following 3 functions:
// Called by leader, when lock held
func (rf *Raft) isNotYetCommitted(logIdx int) bool {
	return logIdx > rf.commitIndex
}
func (rf *Raft) isReplicatedOnMajority(logIdx int) bool {
	majority := len(rf.peers)/2 + 1
	count := 0

	for p := 0; p < len(rf.peers); p++ {
		// increment count if: we're looping over self OR we've this entry is marked as replicated on self
		if p == rf.me || rf.matchIdx[p] >= logIdx {
			count++
		}
	}

	return count >= majority
}
func (rf *Raft) isInSameTerm(logIdx int) bool {
	return rf.log[logIdx].Term == rf.currentTerm
}

func (rf *Raft) apply() {
	for !rf.killed() {
		time.Sleep(time.Duration(20) * time.Millisecond)

		// if commitIdx > lastApplied, send to apply channel
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
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
					rf.matchIdx[rf.me] = rf.commitIndex
					// Usually, we notify nodes of HB on pushLogs or sendHB, but since we'll skip that for ourself, tell ourself to record a HB
					rf.recentHeartbeatReceived = true
				} else if rf.lastLogIdx() >= rf.nextIdx[i] {
					// If we have a new log entry that must be pushed to follower i, spawn agreement process
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
	rf.currentTerm++
	rf.recentHeartbeatReceived = false
	rf.mu.Unlock()

	yesVotes := 0
	voteCount := 0
	var voteMutex sync.Mutex
	cond := sync.NewCond(&voteMutex)

	tempMatchIdx := make([]int, len(rf.peers))
	// Spawn len(rf.peers) goroutines, that each request a vote from a different node
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.mu.Lock()
			rf.votedFor = rf.me
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
						lastLogIdx := rf.lastLogIdx()
						args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIdx, rf.log[lastLogIdx].Term}

						rf.mu.Unlock()

						// Sender (rf) does not hold lock here
						rpcOk := rf.sendRequestVote(nodeIdx, &args, &reply)

						voteMutex.Lock()
						rf.mu.Lock()

						// 2 outcomes for RPC call:
						// 1. success, receive either yes or no vote
						// 2. RPC did not go through. If this is because follower is down, we should retry later so it has a chance to come back up

						if rpcOk {
							// if RPC recipient's term is higher than this candidate's term, we'll have to update cand (soon to be follower, since vote will not have been granted)'s term
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = followerNode
							} else if reply.VoteGranted {
								yesVotes++
							} else {
								rf.state = followerNode
							}

							tempMatchIdx[nodeIdx] = reply.LastLogIdx
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
		rf.state = leaderNode
		// Initialize nextIdx for all followers (and self), to: leader last log index + 1
		// Use tempMatchIdx, which we received from each reply, to init matchIdx for each peer
		initialNextIdx := rf.lastLogIdx() + 1
		rf.nextIdx = make([]int, 0, 5)
		rf.matchIdx = make([]int, 0, 5)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIdx = append(rf.nextIdx, initialNextIdx)
			rf.matchIdx = append(rf.matchIdx, tempMatchIdx[i])
		}
		rf.mu.Unlock()
	}

	voteMutex.Unlock()
}

// send 1 HB to nodeIdx & process response
func (rf *Raft) sendHeartbeatToNode(nodeIdx int) {
	rf.mu.Lock()
	var reply AppendEntriesReply
	// When thinking about PrevLog in a HB - we have 0 entries to apply to follower, so the previous entry is just the last one in this leader's log
	// Important for determining whether to overwrite
	args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.lastLogIdx(), rf.log[rf.lastLogIdx()].Term, make([]*entry, 0), rf.commitIndex}

	rf.mu.Unlock()

	rpcOk := rf.sendAppendEntries(nodeIdx, &args, &reply)

	if !rpcOk {
		// on network failure, downgrade leader to follower
		rf.mu.Lock()
		rf.state = followerNode
		rf.mu.Unlock()
	} else if !reply.Success {
		// RPC goes through but reply.Success == fail
		// -> sender's term was less than receiver's
		// update sender's current term, downgrade to follower
		rf.mu.Lock()
		rf.currentTerm = reply.Term
		rf.state = followerNode
		rf.mu.Unlock()
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
	rf.log = append(rf.log, &entry{nil, 0}) // We want entries to be zero-indexed
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start periodic check for committed logs so they can be applied (this will be more frequent than ticker)
	go rf.apply()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// Should be called when lock held
func (rf *Raft) lastLogIdx() int {
	return len(rf.log) - 1
}
