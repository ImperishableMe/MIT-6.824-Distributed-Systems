package raft


// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d CondInstSnapshot at lastIncludInd-%d, lastIncludTerm-%d, logLastInd-%d T-d",
		rf.me, lastIncludedIndex, lastIncludedTerm, rf.log.lastIndex())

	if rf.log.lastIndex() > lastIncludedIndex {
		Debug(dDrop, "S%d Rejecting old snapshot", rf.me)
		return false
	}
	rf.snapshot = snapshot
	rf.snapshotIndex = lastIncludedIndex
	rf.snapshotTerm = lastIncludedTerm

	rf.log.cutStart(rf.snapshotIndex)
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

	Debug(dSnap, "S%d Snapshot call at ind %d, T-d",
		rf.me, index, rf.currentTerm)

	if index > rf.log.lastIndex() {
		Debug(dError, "S%d (ind,lastInd)-(%d,%d)", rf.me, index, rf.log.lastIndex())
		panic("Advanced log trim")
	}
	rf.log.cutStart(index)
	rf.persistWithSnapshotL()
}
