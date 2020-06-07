package kvraft

import (
	"../labrpc"
	"crypto/rand"
	"math/big"
	"sync"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	sequence int
	mutex    sync.Mutex
	// You will have to modify this struct.

	lastLeader int
	Cid        int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.Cid = nrand()
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
	DPrintf("Clerk [%d] Get %s \n", ck.Cid, key)
	seq := ck.sequence
	ck.sequence++

	args, reply := &GetArgs{
		Key: key,
		Seq: seq,
		Cid: ck.Cid,
	}, &GetReply{}

	curServer := ck.lastLeader
	for {
		if ok := ck.servers[curServer].Call("KVServer.Get", args, reply); ok {
			if reply.Err == ErrWrongLeader {
				curServer = (curServer + 1) % len(ck.servers)
			} else if reply.Err == ErrNoKey {
				ck.lastLeader = curServer
				return ""
			} else if reply.Err == OK {
				ck.lastLeader = curServer
				return reply.Value
			} else if reply.Err == ErrTimeout {
				continue
			}
		}
	}
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
	DPrintf("Clerk [%d] %s, %s = %s \n", ck.Cid, op, key, value)
	seq := ck.sequence
	ck.sequence++

	args, reply := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Seq:   seq,
		Cid:   ck.Cid,
	}, &PutAppendReply{}

	curServer := ck.lastLeader
	for {
		if ok := ck.servers[curServer].Call("KVServer.PutAppend", args, reply); ok {
			if reply.Err == ErrWrongLeader {
				curServer = (curServer + 1) % len(ck.servers)
			} else if reply.Err == OK {
				ck.lastLeader = curServer
				return
			} else if reply.Err == ErrTimeout {
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
