package raft

import (
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term			int 		//  leader's term
	LeaderId 		int 		//	for follower's to redirect client
	PrevLogIndex	int
	PrevLogTerm 	int
	Entries			[]LogEntry
	LeaderCommit 	int
}


func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("T-%v,PLI-%d,PLT-%d, LC-%d",
		a.Term, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit)
}

type AppendEntriesReply struct {
	Term 			int 		// currentTerm, for leader to update itself
	Success			bool 		// true, if follower contained entry matching
	ConflictIndex 	int 		// gives a better nextMatch approximation
}

// Paper check - 2 for fail otherwise continue
func (rf *Raft) handleAppendEntriesConflictL(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	leaderPrevIndex := args.PrevLogIndex
	leaderPrevTerm := args.PrevLogTerm

	Debug(dLog, "S%d LogList %v", rf.me, rf.log)

	if leaderPrevIndex > 0 {
		if leaderPrevIndex > rf.log.lastIndex(){
			// does not contain entry, set conflict to this follower's LogList end
			reply.ConflictIndex = rf.log.lastIndex() + 1
			reply.Success = false
			return true
		}
		if entry := rf.log.entry(leaderPrevIndex); entry.Term != leaderPrevTerm {
			// entry's term does not match, find the first index of this conflicting
			// term's LogList entry in this follower's LogList
			conflictingTerm := entry.Term
			pos := leaderPrevIndex
			for pos > 0 && rf.log.entry(pos).Term == conflictingTerm {
				pos--
			}
			reply.ConflictIndex = pos + 1
			reply.Success = false

			Debug(dInfo, "S%d conflictHandler: confT-%d confI-%d",
				rf.me, conflictingTerm, reply.ConflictIndex)
			return true
		}
	}
	return false
}

func (rf *Raft) AppendEntriesRequestHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dInfo, "S%d <- S%d ApReq T-%d PLI-%d PLT-%d LC-%d",
		rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	Debug(dLog, "S%d <- S%d ApReq Entries: %v", rf.me, args.LeaderId, args.Entries)

	rf.newTermCheckL(args.Term)

	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Term < rf.currentTerm {
		Debug(dTrace, "S%d Got old AEReq", rf.me)
		reply.Success = false
		return
	}

	rf.resettingElectionTimerL() // supposedly got a heartbeat from current leader, setting Timer

	done := rf.handleAppendEntriesConflictL(args, reply)

	if done {      // had a conflict, handled and now return
		return
	}

	// RPC is okay, not update rf.log

	leaderPrevIndex := args.PrevLogIndex
	lastNewInd := leaderPrevIndex

	for ind, entry := range args.Entries {
		curLogIndex := leaderPrevIndex + 1 + ind

		if curLogIndex > rf.log.lastIndex() {
			Debug(dLog2, "S%d adding %d entries at LogList's end(pos %d)",
				rf.me, len(args.Entries[ind:]), rf.log.lastIndex()+1)

			rf.log.appendList(args.Entries[ind:])
			lastNewInd = rf.log.lastIndex()
			break
		} else if rf.log.entry(curLogIndex).Term != entry.Term {
			Debug(dLog2, "S%d LogList cut from pos %d", rf.me, curLogIndex)
			rf.log.cutEnd(curLogIndex)
			Debug(dLog2, "S%d adding %d entries at LogList's pos %d",
				rf.me, len(args.Entries[ind:]), rf.log.lastIndex()+1)

			rf.log.appendList(args.Entries[ind:])
			lastNewInd = rf.log.lastIndex()
			break
		} else {
			lastNewInd = curLogIndex
		}
	}
	Debug(dCommit, "S%d LastNewInd %d", rf.me, lastNewInd)

	// point 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, lastNewInd)
	}
	Debug(dCommit, "S%d commitIndex %d", rf.me, rf.commitIndex)

	rf.persist()
}

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRequestHandler", args, reply)
	return ok
}


func (rf *Raft) appendEntriesDaemon(term int) {

	for !rf.killed() {
		// kick off append entries
		time.Sleep(HeartBeatTimeOut)
		rf.mu.Lock()
		if rf.currentTerm != term || rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		go rf.sendHeartBeat(term)
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartBeat(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for ind, _ := range rf.peers {
		if ind == rf.me {
			continue
		}
		go rf.sendHeartBeatToOne(ind, rf.currentTerm)
	}
}

func (rf *Raft) sendHeartBeatToOne(server, term int)  {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			break
		}
		var entries []LogEntry
		prevTerm , prevLogIndex := 0, 0

		nxtInd := rf.nextIndex[server]

		Debug(dLeader, "S%d -> S%d sending HB at T%d, nxtInd-%d",
			rf.me, server, term, nxtInd)
		Debug(dLog, "S%d LogList-%v", rf.log.LogList)

		if nxtInd == 0 {  	// nxtInd should not be 0
			Debug(dError, "S%d for S%d nxtInd is 0!!", rf.me, server)
			panic("nxtInd is 0")
		}

		prevTerm = rf.log.entry(nxtInd - 1).Term
		prevLogIndex = nxtInd - 1
		entries = make([]LogEntry,len(rf.log.slice(nxtInd))) // empty slice or beyond slice returns 0
		copy(entries, rf.log.slice(nxtInd))

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		Debug(dInfo, "S%d -> S%d sending AERpc %v", rf.me, server, args)
		Debug(dLog, "S%d -> S%d AERpc log- %v", rf.log.LogList)

		reply := AppendEntriesReply{}

		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			continue // appendEntries failed, try again!
		}
		rf.mu.Lock()

		rf.newTermCheckL(reply.Term)

		if term != rf.currentTerm || rf.state != Leader {
			Debug(dInfo, "S%d <- S%d got old AERpc reply.Ignoring!", rf.me, server)
			rf.mu.Unlock()
			break
		}
		if reply.Success {
			potNext := args.PrevLogIndex + len(args.Entries) + 1
			potMatch := potNext - 1

			Debug(dInfo, "S%d <- S%d got successful AERpc reply." +
				"potNxT-%d,potMat-%d, actNxt-%d, actMat-%d",
				rf.me, server, potNext, potMatch, rf.nextIndex[server], rf.matchIndex[server])

			rf.nextIndex[server] = Max(potNext, rf.nextIndex[server])
			rf.matchIndex[server] = Max(potMatch, rf.matchIndex[server])
			rf.mu.Unlock()
			break
		} else {
			Debug(dInfo, "S%d <- S%d got unsuccessful AERpc reply:" +
				"conflictInd-%d, actNxt-%d, actMat-%d",
				rf.me, server, reply.ConflictIndex, rf.nextIndex[server], rf.matchIndex[server])
			rf.nextIndex[server] = reply.ConflictIndex
			rf.mu.Unlock()
		}
	}
}
