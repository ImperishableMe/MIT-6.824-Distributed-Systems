package raft

import "time"

type AppendEntriesArgs struct {
	Term			int 		//  leader's term
	LeaderId 		int 		//	for follower's to redirect client
	PrevLogIndex	int
	PrevLogTerm 	int
	Entries			[]LogEntry
	LeaderCommit 	int
}

type AppendEntriesReply struct {
	Term 			int 		// currentTerm, for leader to update itself
	Success			bool 		// true, if follower contained entry matching
	ConflictIndex 	int 		// gives a better nextMatch approximation
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	noNeedToPersist := rf.preRPCHandler(args.Term)

	if !noNeedToPersist {
		defer rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.resettingElectionTimer() // supposedly got a heartbeat from leader

	leaderPrevIndex := args.PrevLogIndex
	leaderPrevTerm := args.PrevLogTerm

	if leaderPrevIndex >= 0 {
		if leaderPrevIndex >= len(rf.log) {
			reply.ConflictIndex = len(rf.log)
			reply.Success = false
			return
		} else if rf.log[leaderPrevIndex].Term != leaderPrevTerm {
			conflictingTerm := rf.log[leaderPrevIndex].Term
			pos := leaderPrevIndex
			for pos >= 0 && rf.log[pos].Term == conflictingTerm {
				pos--
			}
			reply.ConflictIndex = pos + 1
			reply.Success = false
			return
		}
	}

	lastNewInd := leaderPrevIndex + 1

	for ind, entry := range args.Entries {
		curLogIndex := leaderPrevIndex + 1 + ind
		if curLogIndex >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[ind:]...)
			lastNewInd = len(rf.log) - 1
			break
		} else if rf.log[curLogIndex].Term != entry.Term {
			rf.log = rf.log[:curLogIndex]
			rf.log = append(rf.log, args.Entries[ind:]...)
			lastNewInd = len(rf.log) - 1
			break
		} else {
			lastNewInd = curLogIndex
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, lastNewInd)
	}

	if noNeedToPersist { // has not called defer persist yet :3 I know the code is ugly, but so am I :v
		rf.persist()
	}

	DPrintf("[%d]: Follower\n " +
		"		leader %d : Entries : %v\n" +
		"		leaderCommit : %d\n" +
		"		log : %v\n", rf.me, args.LeaderId, args.Entries,args.LeaderCommit,rf.log)
}

func Min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf *Raft) appendEntriesDaemon() {

	for !rf.killed() {
		// kick off append entries
		time.Sleep(HeartBeatTimeOut)
		rf.mu.Lock()
		if rf.state == Leader {
			go rf.sendHeartBeat()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartBeat() {
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

		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}

		var lastLog LogEntry
		var entries []LogEntry
		prevTerm := -1
		prevLogIndex := -1

		if rf.nextIndex[server] - 1 >= 0 && rf.nextIndex[server] - 1 < len(rf.log) {
			lastLog = rf.log[rf.nextIndex[server]-1]
			prevTerm = lastLog.Term
		}
		if rf.nextIndex[server] >= 0 {
			entries = make([]LogEntry,len(rf.log[rf.nextIndex[server]:]))
			copy(entries, rf.log[rf.nextIndex[server]:])
			prevLogIndex = rf.nextIndex[server] - 1
		} else {
			entries = make([]LogEntry,len(rf.log))
			copy(entries, rf.log)
			prevLogIndex = -1
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}

		//			DPrintf("[%d]Leader to follower-[%d]\n " +
		//				"request: %v\n", rf.me, server, args)
		//			DPrintf("     [%d] Leader log %v\n", rf.me, rf.log)
		//
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			continue // appendEntries failed, try again!
		}
		rf.mu.Lock()

		latestTerm := rf.preRPCHandler(reply.Term)

		if !latestTerm {
			rf.persist()
		}

		if !latestTerm || term != rf.currentTerm || rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			rf.mu.Unlock()
			break
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
			rf.mu.Unlock()
		}
	}
}
