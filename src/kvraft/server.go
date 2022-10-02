package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
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

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	isLeader  					 bool
	keyValue      				 map[string]string // holds the actual key->value
	clientLastOpId				 map[int64]int64    // maps RPC request id to Response
	clientLastOpResponse 		 map[int64]string    // maps RPC request id to Response
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
	clientID, seqNum := op.ClientID, op.SeqNum

	for !kv.killed() {
		kv.mu.Lock()
		lastOpId, ok := kv.clientLastOpId[clientID]
		Debug(dTrace, "S%d ReqID (%d,%d) state-(LastOpId, ok):(%v,%v)", kv.me, clientID, seqNum, lastOpId, ok)

		if !ok || lastOpId < seqNum {
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			Debug(dTimer, "S%d timer expired for ReqId (%d,%d)", kv.me, clientID, seqNum)
		} else if lastOpId > seqNum {
			reply.setErr( "NoLeader")
			Debug(dDrop, "S%d <- Cl(%d) Op(%d) dropped,  %v", kv.me, clientID, seqNum, args)
			kv.mu.Unlock()
			return
		} else {
			reply.setValue(kv.clientLastOpResponse[clientID])
			Debug(dServer, "S%d ReqID (%d,%d) Successfully done. Reply: %v", kv.me, clientID, seqNum, reply)
			kv.mu.Unlock()
			return
		}
	}
	return
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
//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.keyValue = make(map[string]string)
	kv.clientLastOpId = make(map[int64]int64)
	kv.clientLastOpResponse = make(map[int64]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.isLeader = false
	go kv.applier()
	return kv
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		for cmd := range kv.applyCh {
			if cmd.CommandValid {
				kv.mu.Lock()
				kv.executeL(&cmd)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) shouldExecuteL (clientId, seqNum int64) bool {
	lastId, ok := kv.clientLastOpId[clientId]
	return !ok || lastId < seqNum
}

func (kv *KVServer) executeL(cmd *raft.ApplyMsg) {
	op, ok := cmd.Command.(Op)
	if !ok {
		panic("Command Type Conversion Problem!")
	}
	Debug(dServer, "S%d trying to apply Op %v", kv.me, op)
	key, value := op.Key, op.Value
	//id := getHashcode(op.ClientID, op.SeqNum)
	if !kv.shouldExecuteL(op.ClientID, op.SeqNum) {
		Debug(dDrop, "S%d refused to reapply Op %v", kv.me, op)
		return
	}
	Debug(dServer, "S%d executes Op %v", kv.me, op)
	switch op.CmdType {
	case "Get":
	case "Put":
		kv.keyValue[key] = value
	case "Append":
		kv.keyValue[key] += value
	}
	kv.clientLastOpId[op.ClientID] = op.SeqNum
	kv.clientLastOpResponse[op.ClientID] = kv.keyValue[key]

	Debug(dServer, "S%d values of states (clntLast, lastResponse): (%v,%v)",
		kv.me, kv.clientLastOpId[op.ClientID], kv.clientLastOpResponse[op.ClientID])
}
