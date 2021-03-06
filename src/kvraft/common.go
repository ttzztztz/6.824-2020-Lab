package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

const WaitCmdTimeOut = time.Millisecond * 500

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Seq   int
	Cid   int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Seq int
	Cid int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type LastReply struct {
	Seq   int
	Value string
	Err   Err
}
