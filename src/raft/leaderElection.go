package raft

import (
	"math/rand"
	"time"
)

const (
	HeartBeatTimeOut = 100 * time.Millisecond
	ElectionTimeOutMin = 400
	ElectionTimeOutMax = 700
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 				int  	// candidate's term
	CandidateId 		int   	// candidate requesting vote
	LastLogIndex 		int 	// index of candidate's last log entry
	LastLogTerm 		int 	// term of candidate's last log entry
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term				int	// currentTerm, for candidate to update itself
	VoteGranted 		bool 	// true means candidates received vote
}

// RequestVoteHandler
// example RequestVoteHandler RPC handler.
//
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.newTermCheckL(args.Term)
	//noNeedToPersist := rf.preRPCHandlerL(args.Term) // update current term, votedFor

	reply.Term = rf.currentTerm
	reply.VoteGranted = false


	if args.Term < rf.currentTerm {
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// did not vote for this term or has voted this candidate, repeated req
		isEmpty := len(rf.log) == 0
		if isEmpty {
			reply.VoteGranted = true
		} else {
			// not empty
			myLastLog := rf.log[len(rf.log) - 1]

			if myLastLog.Term < args.LastLogTerm ||
				myLastLog.Term == args.LastLogTerm &&
					len(rf.log) <= (1 + args.LastLogIndex) { // 0-based log

				reply.VoteGranted = true
			}
		}
	}

	if reply.VoteGranted { // voting the for the candidate
		rf.votedFor = args.CandidateId
		rf.resettingElectionTimerL() // resetting the timer
		rf.persist()
	}


}

//
// example code to send a RequestVoteHandler RPC to a server.
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
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	return ok
}

// New term found
func (rf *Raft) newTermCheckL(foundTerm int) {
	if foundTerm > rf.currentTerm {
		Debug(dInfo, "S%d found new Term T%d, L%d",
			rf.me, foundTerm, rf.currentTerm)

		rf.currentTerm = foundTerm
		rf.becomeFollowerL()
		rf.persist()
	}
}

// will be called holding the lock
func (rf *Raft) resettingElectionTimerL() {
	Debug(dTimer, "S%d is resetting Election timer.", rf.me)
	rf.lastHeartBeat = time.Now()
	rf.electionTimeOut = rand.Intn(ElectionTimeOutMax - ElectionTimeOutMin) +
		ElectionTimeOutMin
}

// It will always be called holding mu lock
func (rf *Raft) preRPCHandlerL(foundTerm int) bool {
	if foundTerm > rf.currentTerm {
		rf.currentTerm = foundTerm
		rf.becomeFollowerL()
		rf.persist()
		return false
	}
	return true
}
// Called after holding mu lock

func (rf *Raft) becomeLeaderL(term int) {

	Debug(dTerm,"S%d becomes leader on T%d", rf.me, term)

	rf.state = Leader
	rf.resettingElectionTimerL()

	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}
		rf.nextIndex[ind] = len(rf.log) // FIXME log
		rf.matchIndex[ind] = -1 		// FIXME log
	}
	go rf.sendHeartBeat(term)
	go rf.updateLeaderCommitIndex(term)
	// need some nextInd resetting for 2B, 2C
}

// Also called holding the mu lock
func (rf *Raft) becomeFollowerL(){
	Debug(dInfo, "S%d -> follower at T%d", rf.me, rf.currentTerm)
	rf.votedFor = -1
	rf.state = Follower
}


func (rf *Raft) electionDaemon() {

	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		if time.Since(rf.lastHeartBeat).Milliseconds() >
			int64(rf.electionTimeOut) {

			if rf.state != Leader {
				go rf.kickOffAnElection()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) prepareForAnElection() {
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.currentTerm++
	Debug(dVote, "S%d ELT elapsed. Candidate at T%d",
		rf.me, rf.currentTerm)
	rf.resettingElectionTimerL()
	rf.persist()
}

func (rf *Raft) kickOffAnElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.prepareForAnElection()

	voteCount := 1  // voting for itself
	setLeader := 0 	// for checking at most once it is set leader at this term

	askedVoteTerm := rf.currentTerm // for local use
	lastLogIndex, lastLogTerm := -1, -1 // FIXME log

	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIndex].Term
	}


	for ind,_ := range rf.peers {
		if ind == rf.me {
			continue
		}
		go func(curInd, lastLogIndex, lastLogTerm int){
			args := RequestVoteArgs{
				Term:         askedVoteTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(curInd, &args, &reply)
			if !ok {return}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.newTermCheckL(reply.Term)

			if rf.currentTerm != askedVoteTerm || rf.state != Candidate {
				return
			}
			if reply.VoteGranted {
				Debug(dVote, "S%d <- S%d Got vote", rf.me, curInd)
				voteCount++
				if voteCount * 2 > len(rf.peers) {
					// won the selection
					rf.becomeLeaderL(rf.currentTerm)

					setLeader++
					// failing explicitly
					if setLeader > 1 {
						Debug(dError, "S%d at T%d selected leader more than once",
							rf.me, rf.currentTerm)
						panic("Leader more than once in a term")
					}
				}

			}
		}(ind, lastLogIndex, lastLogTerm)
	}
}
