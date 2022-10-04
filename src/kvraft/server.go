package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CmdType  string
	Key      string
	Value    string
	ClientID int64
	SeqNum   int64
}

type ChannelInfo struct {
	op    Op
	value string
	index int
}

func (op Op) String() string {
	return fmt.Sprintf("(Cid:%v,seqN:%v,cmdType:%v,key:%v,val:%v)",
		op.ClientID, op.SeqNum, op.CmdType, op.Key, op.Value)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big (-1 means no snapshot)
	persister    *raft.Persister

	// Your definitions here.
	Db                   map[string]string // holds the actual key->value
	ClientLastOpId       map[int64]int64   // maps RPC request id to Response
	ClientLastOpResponse map[int64]string  // maps RPC request id to Response
	cmdIndexDispatcher   map[int][]chan ChannelInfo
}

func (kv *KVServer) isDuplicateL(index, term int, op *Op, reply Reply) bool {
	lastOpId, ok := kv.ClientLastOpId[op.ClientID]
	Debug(dTrace, "S%d ReqID (%d,%d) state-(LastOpId, ok):(%v,%v)", kv.me, op.ClientID, op.SeqNum, lastOpId, ok)

	if !ok {
		return false
	}

	if lastOpId > op.SeqNum {
		reply.setErr("NoLeader")
		Debug(dDrop, "S%d <- Cl(%d) Op(%d) dropped,  %v", kv.me, op.ClientID, op.SeqNum, op)
		return true
	} else if lastOpId == op.SeqNum {
		reply.setValue(kv.ClientLastOpResponse[op.ClientID])
		Debug(dServer, "S%d ReqID (%d,%d) Successfully done. Reply: %v", kv.me, op.ClientID, op.SeqNum, reply)
		return true
	}
	return false
}

func (kv *KVServer) handler(op Op, args interface{}, reply Reply) {
	Debug(dServer, "S%d <- Cl(%d) Op received %v", kv.me, op.ClientID, op)
	index, term, isLeader := kv.rf.Start(op)

	Debug(dServer, "S%d Op(%v) status: (ind,term,isL):(%v,%v,%v) ", kv.me, op, index, term, isLeader)
	if kv.shouldDrop(isLeader) {
		reply.setErr("NotLeader")
		Debug(dDrop, "S%d <- Cl(%d) Op(%v) dropped", kv.me, op.ClientID, op)
		return
	}
	//id := getHashcode(args.ClientId, args.SeqNum)

	kv.mu.Lock()
	duplicate := kv.isDuplicateL(index, term, &op, reply)
	if duplicate {
		kv.mu.Unlock()
		return
	}

	_, ok := kv.cmdIndexDispatcher[index]
	if !ok {
		kv.cmdIndexDispatcher[index] = make([]chan ChannelInfo, 0)
	}
	kv.cmdIndexDispatcher[index] = append(kv.cmdIndexDispatcher[index], make(chan ChannelInfo, 1))
	infoChan := kv.cmdIndexDispatcher[index][len(kv.cmdIndexDispatcher[index])-1]
	kv.mu.Unlock()

	select {
	case info := <-infoChan:
		isInvalid := info.op.CmdType == "" || // channel closed
			info.op.ClientID != op.ClientID || info.op.SeqNum != op.SeqNum

		if isInvalid {
			reply.setErr("NotLeader")
		} else {
			reply.setValue(info.value)
		}
	case <-time.After(time.Millisecond * 500):
		kv.mu.Lock()
		defer kv.mu.Unlock()

		duplicate := kv.isDuplicateL(index, term, &op, reply)
		if duplicate {
			return
		}
		reply.setErr("NotLeader!")
	}
}

func (kv *KVServer) shouldDrop(isLeader bool) bool {
	return !isLeader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{CmdType: "Get", SeqNum: args.SeqNum, ClientID: args.ClientId, Key: args.Key}
	kv.handler(op, args, reply)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{CmdType: args.Op, SeqNum: args.SeqNum, ClientID: args.ClientId, Key: args.Key, Value: args.Value}
	kv.handler(op, args, reply)
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()

	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.
	kv.Db = make(map[string]string)
	kv.ClientLastOpId = make(map[int64]int64)
	kv.ClientLastOpResponse = make(map[int64]string)
	kv.cmdIndexDispatcher = make(map[int][]chan ChannelInfo)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// TODO: read from persistent snapshot
	kv.restoreState()
	go kv.applier()
	return kv
}

func (kv *KVServer) restoreState() {
	snap := kv.persister.ReadSnapshot()

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
		kv.readSnapshotL(tSnap)
	}
}

func (kv *KVServer) createSnapshotL() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.Db)
	e.Encode(kv.ClientLastOpId)
	e.Encode(kv.ClientLastOpResponse)

	data := w.Bytes()
	return data
}

func (kv *KVServer) readSnapshotL(snap []byte) {
	if snap == nil || len(snap) < 1 {
		return
	}

	r := bytes.NewBuffer(snap)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var cid map[int64]int64   // maps RPC request id to Response
	var crep map[int64]string // maps RPC request id to Response

	if d.Decode(&db) != nil ||
		d.Decode(&cid) != nil ||
		d.Decode(&crep) != nil {
	} else {
		kv.Db = db
		kv.ClientLastOpId = cid
		kv.ClientLastOpResponse = crep
	}
}

func (kv *KVServer) applier() {
	lastApplied := 0

	for cmd := range kv.applyCh {
		if cmd.CommandValid {
			kv.mu.Lock()
			kv.executeL(&cmd)
			lastApplied = cmd.CommandIndex
			kv.mu.Unlock()
		} else if cmd.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(cmd.SnapshotTerm, cmd.SnapshotIndex, cmd.Snapshot) {
				// no need to clear the pending reqs, they will either time out and get handled
				kv.readSnapshotL(cmd.Snapshot)
			}
			kv.mu.Unlock()
		}

		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			// TODO: take a snapshot and pass it to raft using
			// call rf.Snapshot()
			kv.mu.Lock()
			snap := kv.createSnapshotL()
			kv.mu.Unlock()
			kv.rf.Snapshot(lastApplied, snap)
		}
	}

	// closes the open channel as kv is dead (applyCh is closed by raft when server is dead)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, chlist := range kv.cmdIndexDispatcher {
		for _, ch := range chlist {
			close(ch)
		}
	}
}

func (kv *KVServer) shouldExecuteL(clientId, seqNum int64) bool {
	lastSeq, ok := kv.ClientLastOpId[clientId]
	return !ok || lastSeq < seqNum
}

func (kv *KVServer) executeL(cmd *raft.ApplyMsg) {
	op, ok := cmd.Command.(Op)
	if !ok {
		panic("Command Type Conversion Problem!")
	}
	Debug(dServer, "S%d trying to apply Op %v", kv.me, op)

	defer func() {
		for _, ch := range kv.cmdIndexDispatcher[cmd.CommandIndex] {
			if kv.ClientLastOpId[op.ClientID] != op.SeqNum {
				panic("Does not have response")
			}
			ch <- ChannelInfo{op, kv.ClientLastOpResponse[op.ClientID], cmd.CommandIndex}
			close(ch)
		}
		delete(kv.cmdIndexDispatcher, cmd.CommandIndex)
	}()

	key, value := op.Key, op.Value
	if !kv.shouldExecuteL(op.ClientID, op.SeqNum) {
		Debug(dDrop, "S%d refused to reapply Op %v", kv.me, op)
		return
	}
	Debug(dServer, "S%d executes Op %v", kv.me, op)

	switch op.CmdType {
	case "Get":
	case "Put":
		kv.Db[key] = value
	case "Append":
		kv.Db[key] += value
	}

	kv.ClientLastOpId[op.ClientID] = op.SeqNum
	kv.ClientLastOpResponse[op.ClientID] = kv.Db[key]

	Debug(dServer, "S%d values of states (clntLast, lastResponse): (%v,%v)",
		kv.me, kv.ClientLastOpId[op.ClientID], kv.ClientLastOpResponse[op.ClientID])
}
