package raft

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term              int // currentTerm, for candidate to update itself
	LastIncludedIndex int // needs this to update the leader properly
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotHandler", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dSnap, "S%d got InstallSnapshot RPC, (snapI, snapT, snapLen) - (%d, %d, %d)",
		rf.me, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))

	rf.newTermCheckL(args.Term)
	reply.Term = rf.currentTerm
	reply.LastIncludedIndex = args.LastIncludedIndex

	isNewSnapshot := rf.waitingSnapshot == nil ||
		rf.waitingSnapshotTerm < rf.currentTerm || // snap of a previous Term ?
		rf.waitingSnapshotIndex < args.LastIncludedIndex // snap of this term but old

	Debug(dSnap, "S%d waitingSnapState: (Ind, Term, len): (%d, %d, %d)",
		rf.me, rf.waitingSnapshotIndex, rf.waitingSnapshotTerm, len(rf.waitingSnapshot))

	if isNewSnapshot {
		rf.waitingSnapshot = args.Data
		rf.waitingSnapshotIndex = args.LastIncludedIndex
		rf.waitingSnapshotTerm = args.LastIncludedTerm

		rf.applierCond.Broadcast()

		Debug(dSnap, "S%d accepted this snapshot", rf.me)
	} else {
		Debug(dSnap, "S%d rejected this snapshot", rf.me)
	}
}

func (rf *Raft) installSnapshotL(server, term int) {
	args := InstallSnapshotArgs{
		Data:              rf.snapshot,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
	}
	reply := InstallSnapshotReply{}
	Debug(dSnap, "S%d -> S%d sending InstallSnapshot RPC, (snapI, snapT, snapLen) - (%d, %d, %d)",
		rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))

	rf.mu.Unlock()

	ok := rf.sendInstallSnapshotRPC(server, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.newTermCheckL(reply.Term)
	if term != rf.currentTerm || rf.state != Leader {
		Debug(dInfo, "S%d <- S%d got old ISRpc reply.Ignoring!", rf.me, server)
		return
	}
	Debug(dSnap, "S%d <- S%d got successful ISRpc reply. LastIncludedInd: %d", rf.me, server, reply.LastIncludedIndex)

	rf.nextIndex[server] = Max(rf.nextIndex[server], reply.LastIncludedIndex+1)
	rf.matchIndex[server] = Max(rf.matchIndex[server], reply.LastIncludedIndex)
	rf.updateCommitCond.Broadcast()
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d CondInstSnapshot from service at lastIncludInd-%d, lastIncludTerm-%d, logLastInd-%d T-d",
		rf.me, lastIncludedIndex, lastIncludedTerm, rf.log.lastIndex())

	if rf.lastApplied > lastIncludedIndex { // TODO: check against appliedIndex
		Debug(dDrop, "S%d Rejecting old snapshot", rf.me)
		return false
	}

	rf.snapshot = snapshot
	rf.snapshotIndex = lastIncludedIndex
	rf.snapshotTerm = lastIncludedTerm

	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	Debug(dSnap, "S%d Log Before cutting at %d:  %v", rf.me, rf.snapshotIndex, rf.log)

	if lastIncludedIndex > rf.log.lastIndex() {
		rf.log = mkLog(
			[]LogEntry{{Cmd: nil, Term: lastIncludedTerm}},
			lastIncludedIndex,
		)
	} else {
		rf.log.cutStart(rf.snapshotIndex)
	}
	Debug(dSnap, "S%d Log After cutting at %d:  %v", rf.me, rf.snapshotIndex, rf.log)
	rf.persistWithSnapshotL()
	return true
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the LogList through (and including)
// that index. Raft should now trim its LogList as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastEntry := rf.log.entry(index)

	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = lastEntry.Term

	Debug(dSnap, "S%d Snapshot call from service at ind %d, T%d, snap-len:%d",
		rf.me, index, rf.currentTerm, len(snapshot))

	if index > rf.log.lastIndex() {
		Debug(dError, "S%d (ind,lastInd)-(%d,%d)", rf.me, index, rf.log.lastIndex())
		panic("Advanced log trim")
	}
	Debug(dSnap, "S%d Log Before cutting at %d:  %v", rf.me, index, rf.log)

	if index > rf.log.lastIndex() {
		panic("Snapshot index problem!")
	}

	rf.log.cutStart(index)
	Debug(dSnap, "S%d Log After cutting at %d:  %v", rf.me, index, rf.log)
	rf.persistWithSnapshotL()
}
