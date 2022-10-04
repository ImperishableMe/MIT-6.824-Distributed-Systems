package raft

type State string

const (
	Leader    State = "Leader"
	Candidate State = "Candidate"
	Follower  State = "Follower"
)

// ApplyMsg
// as each Raft peer becomes aware that successive LogList entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed LogList entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) applyDaemon(applyCh chan ApplyMsg) {

	defer close(applyCh)
	rf.mu.Lock()

	for {
		if rf.killed() {
			rf.mu.Unlock()
			break
		}

		if rf.waitingSnapshot != nil {
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.waitingSnapshot,
				SnapshotTerm:  rf.waitingSnapshotTerm,
				SnapshotIndex: rf.waitingSnapshotIndex,
			}
			rf.waitingSnapshot = nil
			rf.mu.Unlock()
			applyCh <- msg
			rf.mu.Lock()
		}
		// log is acquired

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.entry(rf.lastApplied).Cmd,
				CommandIndex: rf.lastApplied,
			}
			Debug(dCommit, "S%d applied new cmd-%v, cmdInd-%d",
				rf.me, msg.Command, msg.CommandIndex)

			rf.mu.Unlock()
			applyCh <- msg
			rf.mu.Lock()
		}

		rf.applierCond.Wait()
	}
}
