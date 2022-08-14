package raft

import (
	"6.824/labgob"
	"bytes"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	Debug(dPersist, "S%d persisting raft State", rf.me)
	Debug(dPersist, "S%d reading state." +
		"curT:%d,votT:%d,log:(ind0,lastInd):(%d,%d)",
		rf.me, rf.currentTerm, rf.votedFor, rf.log.Ind0, rf.log.lastIndex())

	data := rf.myStateEncoder()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistWithSnapshotL() {
	Debug(dPersist, "S%d persisting raft State and Snapshot", rf.me)
	Debug(dPersist, "S%d reading state." +
		"curT:%d,votT:%d,log:(ind0,lastInd):(%d,%d)",
		rf.me, rf.currentTerm, rf.votedFor, rf.log.Ind0, rf.log.lastIndex())
	Debug(dPersist, "S%d reading SnapShot." +
		"shotLen:%d,sInd:%d,sTerm:%d",
		rf.me, len(rf.snapshot), rf.snapshotIndex, rf.snapshotTerm)

	data := rf.myStateEncoder()
	snap := rf.mySnapshotEncoder()

	rf.persister.SaveStateAndSnapshot(data, snap)
}

func (rf *Raft) myStateEncoder() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	return data
}

func (rf *Raft) mySnapshotEncoder() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.snapshot)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)

	data := w.Bytes()
	return data
}
//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log Log

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log

		Debug(dPersist, "S%d reading state." +
			"curT:%d,votT:%d,log:%v",
			rf.me, rf.currentTerm, rf.votedFor, rf.log)
	}
	rf.readPersistSnapshot()
}

// read the persisted snapshot
func (rf *Raft) readPersistSnapshot(){
	snap := rf.persister.ReadSnapshot()
	if snap == nil || len(snap) < 1 { // bootstrap without any state?
		return
	}
	var tSnap []byte
	var tSnapInd, tSnapTerm int

	sr := bytes.NewBuffer(snap)
	sd := labgob.NewDecoder(sr)

	if sd.Decode(&tSnap) != nil ||
		sd.Decode(&tSnapInd) != nil ||
		sd.Decode(&tSnapTerm) != nil {
	} else {
		rf.snapshot = tSnap
		rf.snapshotIndex = tSnapInd
		rf.snapshotTerm = tSnapTerm

		Debug(dPersist, "S%d reading SnapShot." +
			"shotLen:%d,sInd:%d,sTerm:%d",
			rf.me, len(rf.snapshot), rf.snapshotIndex, rf.snapshotTerm)
	}
}
