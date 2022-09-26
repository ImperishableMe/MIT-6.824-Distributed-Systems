package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValue        map[string]string // holds the actual key->value
	requestResponse map[string]string // maps RPC request id to Response
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	_, _, isLeader := kv.rf.Start(Op{CmdType: "Get", SeqNum: args.SeqNum, ClientID: args.ClientId, Key: args.Key})
	kv.mu.Unlock()
	if !isLeader {
		reply.Err = "NotLeader"
		return
	}
	id := getHashcode(args.ClientId, args.SeqNum)

	for !kv.killed() {
		kv.mu.Lock()
		response, ok := kv.requestResponse[id]
		if !ok {
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		} else {
			reply.Value = response
			kv.mu.Unlock()
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	_, _, isLeader := kv.rf.Start(Op{CmdType: args.Op, SeqNum: args.SeqNum, ClientID: args.ClientId, Key: args.Key, Value: args.Value})
	kv.mu.Unlock()
	if !isLeader {
		reply.Err = "NotLeader"
		return
	}
	id := getHashcode(args.ClientId, args.SeqNum)

	for !kv.killed() {
		kv.mu.Lock()
		_, ok := kv.requestResponse[id]
		if !ok {
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		} else {
			kv.mu.Unlock()
			return
		}
	}
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
	kv.requestResponse = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
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

func (kv *KVServer) executeL(cmd *raft.ApplyMsg) {
	op, ok := cmd.Command.(Op)
	if !ok {
		panic("Command Type Conversion Problem!")
	}

	key, value := op.Key, op.Value
	id := getHashcode(op.ClientID, op.SeqNum)

	switch op.CmdType {
	case "Get":
	case "Put":
		kv.keyValue[key] = value
	case "Append":
		kv.keyValue[key] += value
	}
	kv.requestResponse[id] = kv.keyValue[key]
}
