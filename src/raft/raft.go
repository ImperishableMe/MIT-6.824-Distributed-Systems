package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new LogList entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the LogList, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// persistent
	currentTerm int
	votedFor    int // which peer got vote from me in currentTerm (votedFor can be me)
	log         Log // first index is 1

	// volatile
	commitIndex int // index of highest log entry known to be committed (
	// initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine ( initialized to 0)

	state           State // current State of the raft instance
	lastHeartBeat   time.Time
	electionTimeOut int

	// Leader's only attributes
	nextIndex  []int
	matchIndex []int

	// SnapShot states
	snapshot      []byte
	snapshotIndex int // includes upto(including) this term
	snapshotTerm  int // corresponding term

	// temporary snapshot state for applying
	waitingSnapshot      []byte
	waitingSnapshotIndex int // includes upto(including) this term
	waitingSnapshotTerm  int // corresponding term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == Leader

	return term, isLeader
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's LogList. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft LogList, since the leader
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

	index := -1
	term := -1
	isLeader := rf.state == Leader

	if !isLeader {
		return index, term, isLeader
	}

	index = rf.log.lastIndex() + 1
	rf.log.append(LogEntry{
		Cmd:  command,
		Term: rf.currentTerm,
	})
	term = rf.currentTerm

	Debug(dClient, "S%d New Command T:%d cmd: %v,"+
		"trying ind:%d",
		rf.me, rf.currentTerm, command, index)

	rf.persistWithSnapshotL()
	go rf.sendHeartBeat(false)

	return index, term, isLeader
}

// Kill
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

// Make
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
	rf.state = Follower
	rf.votedFor = -1
	rf.log = mkLogEmpty()

	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = rf.snapshotIndex // TODO: initialize properly after snapshotting
	rf.lastApplied = rf.snapshotIndex // TODO: initialize properly after snapshotting

	Debug(dInfo, "S%d is live now at T:%d, VotF:%d",
		rf.me, rf.currentTerm, rf.votedFor)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.resettingElectionTimerL()

	// initialize from state persisted before a crash

	if rf.waitingSnapshot != nil {
		panic("Rogue WaitingSnapshot!")
	}

	go rf.electionDaemon()
	go rf.applyDaemon(applyCh)

	return rf
}

func (rf *Raft) updateLeaderCommitIndex(term int) {

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			break
		}
		for i := rf.commitIndex + 1; i <= rf.log.lastIndex(); i++ {
			if rf.log.entry(i).Term != rf.currentTerm {
				continue
			}
			count := 1 // leader matched for sure
			for ind, val := range rf.matchIndex {
				if ind == rf.me {
					continue
				}
				if val >= i {
					count++
				}
				if count*2 > len(rf.peers) {
					rf.commitIndex = i
					Debug(dCommit, "S%d leader at T%d committed upto %d\n",
						rf.me, rf.currentTerm, rf.commitIndex)
					break
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
