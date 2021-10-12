package raft

import (
	"math/rand"
	"time"
)

const (
	HeartBeatTimeOut = 100 * time.Millisecond
	ElectionTimeOutMin = 400
	ElectionTimeOutMax = 500
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


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	noNeedToPersist := rf.preRPCHandler(args.Term) // update current term, votedFor

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	isConsistent := true

	if args.Term < rf.currentTerm {
		isConsistent = false
	}
	if isConsistent && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
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

	if isConsistent && reply.VoteGranted { // voting the for the candidate
		rf.votedFor = args.CandidateId
		rf.resettingElectionTimer() // resetting the timer
		noNeedToPersist = false
	}

	if !noNeedToPersist {
		rf.persist()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


// will be called holding the lock
func (rf *Raft) resettingElectionTimer() {
	rf.lastHeartBeat = time.Now()
	rf.electionTimeOut = rand.Intn(ElectionTimeOutMax - ElectionTimeOutMin) +
		ElectionTimeOutMin
}

// It will always be called holding mu lock
func (rf *Raft) preRPCHandler(foundTerm int) bool {
	if foundTerm > rf.currentTerm {
		rf.currentTerm = foundTerm
		rf.becomeFollower()
		return false
	}
	return true
}
// Called after holding mu lock

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.resettingElectionTimer()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for ind, _ := range rf.nextIndex {
		rf.nextIndex[ind] = len(rf.log)
	}
	for ind, _ := range rf.matchIndex {
		rf.matchIndex[ind] = -1
	}

	go rf.updateLeaderCommitIndex()
	// need some nextInd resetting for 2B, 2C
}

// Also called holding the mu lock
func (rf *Raft) becomeFollower(){
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
	rf.resettingElectionTimer()
}

func (rf *Raft) kickOffAnElection() {
	rf.mu.Lock()

	rf.prepareForAnElection()
	voteCount := 1  // voting for itself
	result := 1

	DPrintf("[%d] Starting An election at term #%d", rf.me, rf.currentTerm)
	askedVoteTerm := rf.currentTerm // for local use
	lastLogIndex, lastLogTerm := -1, -1

	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	rf.persist()
	rf.mu.Unlock()

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
			if !ok {		// error in the RPC, so no vote :3
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			result++

			noNeedToPersist := rf.preRPCHandler(reply.Term) // check whether you had an old term

			if !noNeedToPersist {
				rf.persist()
			}

			if rf.currentTerm != askedVoteTerm || rf.state != Candidate {
				return
			}
			if reply.VoteGranted {
				voteCount++  // use synchronous variable here
			}
		}(ind, lastLogIndex, lastLogTerm)
	}

	for {
		rf.mu.Lock()
		if voteCount * 2 < len(rf.peers) && result < len(rf.peers) {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	defer rf.mu.Unlock()

	if rf.state != Candidate || rf.currentTerm != askedVoteTerm {
		return
	}
	if voteCount * 2 >= len(rf.peers) {
		// won the selection
		rf.becomeLeader()
		DPrintf("[%d] is becoming the leader at term #%d", rf.me, rf.currentTerm)
		go rf.sendHeartBeat()
		//
	} else {
		// become follower
		rf.becomeFollower()
		DPrintf("[%d] lost the election at term #%d", rf.me, rf.currentTerm)
	}

	rf.persist()
}
