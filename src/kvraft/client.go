package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers     []*labrpc.ClientEnd
	leaderIndex int
	ClientId    int64
	SeqNum      int64
	// You will have to modify this struct.
}

const Sleep bool = false

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ClientId = nrand()
	ck.SeqNum = 0        // starting from 1 won't be a problem because each instance of client gets a unique (hopefully) ID
	ck.leaderIndex = 0
	Debug(dTest, "Client with ID %d started!", ck.ClientId)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.SeqNum++
	for i := ck.leaderIndex; i < len(ck.servers); i = (i+1) % len(ck.servers) {
		args, reply := GetArgs{key, ck.ClientId, ck.SeqNum}, GetReply{}
		Debug(dClient, "S%d <- Cl(%d) req Op %v", i, ck.ClientId, args)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == "" {
			ck.leaderIndex = i
			Debug(dClient, "S%d -> Cl(%d) Got Succ Op seq:%d %v", i, ck.ClientId, args.SeqNum ,reply)
			return reply.Value
		}
		if Sleep {
			time.Sleep(5 * time.Millisecond)
		}
	}
	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.SeqNum++
	for i := ck.leaderIndex; i < len(ck.servers); i = (i+1) % len(ck.servers) {
		args, reply := PutAppendArgs{
			key,
			value,
			op,
			ck.ClientId,
			ck.SeqNum,
		}, PutAppendReply{}
		Debug(dClient, "S%d <- Cl(%d) req Op %v", i, ck.ClientId, args)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == "" {
			Debug(dClient, "S%d -> Cl(%d) Got Succ Op seq:%d %v", i, ck.ClientId, args.SeqNum ,reply)
			ck.leaderIndex = i
			return
		}
		if Sleep {
			time.Sleep(5 * time.Millisecond)
		}
	}
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
