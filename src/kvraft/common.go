package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	Op    		string // "Put" or "Append"
	ClientId 	int64
	SeqNum 		int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key 		string
	ClientId 	int64
	SeqNum 		int64
	// You'll have to add definitions here.
}


type GetReply struct {
	Err   Err
	Value string
}


func getHashcode (clientId, seqNum int64) string {
	return fmt.Sprintf("%v,%v", clientId, seqNum)
}

func getId (code string) (int64, int64) {
	var clientId, seqNum int64
	_, err := fmt.Sscanf(code, "%d,%d", &clientId, &seqNum)
	if err != nil { panic("invalid decoding")}
	return clientId, seqNum
}