package raft

import (
	"math/rand"
	"time"
)

const (
	HeartBeatTimeOut   = 100 * time.Millisecond
	ElectionTimeOutMin = 400
	ElectionTimeOutMax = 700
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last LogList entry
	LastLogTerm  int // term of candidate's last LogList entry
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidates received vote
}

// RequestVoteHandler
// example RequestVoteHandler RPC handler.
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dVote, "S%d <- S%d askedVote,info: T-%d LI-%d, LT-%d",
		rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)

	rf.newTermCheckL(args.Term)
	//noNeedToPersist := rf.preRPCHandlerL(args.Term) // update current term, votedFor

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// did not vote for this term or has voted this candidate, repeated req
		myLastLog := rf.log.lastEntry()
		isUptoDate := myLastLog.Term < args.LastLogTerm ||
			myLastLog.Term == args.LastLogTerm &&
				rf.log.lastIndex() <= args.LastLogIndex

		if isUptoDate { // voting the for the candidate
			Debug(dVote, "S%d -> S%d granted vote, granters state: T%d, LT-%d, LI-%d",
				rf.me, args.CandidateId, rf.currentTerm, myLastLog.Term, rf.log.lastIndex())

			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.resettingElectionTimerL() // resetting the timer
			rf.persistWithSnapshotL()    // TODO: fix persisting with snapshot
			return
		}
	}
	Debug(dVote, "S%d <- S%d vote denied", rf.me, args.CandidateId)
}

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
		rf.persistWithSnapshotL()
	}
}

// will be called holding the lock
func (rf *Raft) resettingElectionTimerL() {
	Debug(dTimer, "S%d is resetting Election timer.", rf.me)
	// rf.lastHeartBeat = time.Now().Add(-ElectionTimeOutMin * time.Millisecond)
	rf.lastHeartBeat = time.Now()
	rf.electionTimeOut = rand.Intn(ElectionTimeOutMax-ElectionTimeOutMin) +
		ElectionTimeOutMin
}

func (rf *Raft) becomeLeaderL(term int) {

	Debug(dTerm, "S%d becomes leader on T%d", rf.me, term)

	rf.state = Leader
	rf.resettingElectionTimerL()

	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}
		rf.nextIndex[ind] = rf.log.lastIndex() + 1
		rf.matchIndex[ind] = 0
	}
	go rf.sendHeartBeat(true)
	go rf.appendEntriesDaemon(term)
	go rf.updateLeaderCommitIndex(term)
	// need some nextInd resetting for 2B, 2C
}

func (rf *Raft) becomeFollowerL() {
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
	rf.persistWithSnapshotL()
}

func (rf *Raft) kickOffAnElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.prepareForAnElection()

	voteCount := 1 // voting for itself
	setLeader := 0 // for checking at most once it is set leader at this term

	askedVoteTerm := rf.currentTerm // for local use
	lastLogIndex := rf.log.lastIndex()
	lastLogTerm := rf.log.lastEntry().Term

	for ind, _ := range rf.peers {
		if ind == rf.me {
			continue
		}

		go func(curInd, lastLogIndex, lastLogTerm int) {
			args := RequestVoteArgs{
				Term:         askedVoteTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(curInd, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.newTermCheckL(reply.Term)

			if rf.currentTerm != askedVoteTerm || rf.state != Candidate {
				return
			}
			if reply.VoteGranted {
				Debug(dVote, "S%d <- S%d Got vote", rf.me, curInd)
				voteCount++
				if voteCount*2 > len(rf.peers) {
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
