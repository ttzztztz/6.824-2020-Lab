package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	sequence int
	mutex    sync.Mutex
	// You will have to modify this struct.
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
	args, reply := &GetArgs{
		Key: key,
	}, &GetReply{}

	for _, item := range ck.servers {
		okChan := make(chan bool)
		go func() {
			okChan <- item.Call("KVServer.Get", &args, &reply)
		}()

		select {
		case result := <-okChan:
			if result {
				return reply.Value
			}
		case <-time.After(300 * time.Millisecond):
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
	ck.mutex.Lock()
	seq := ck.sequence
	ck.sequence++
	ck.mutex.Unlock()

	args, reply := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Sequence: seq,
	}, &PutAppendReply{}

	for _, item := range ck.servers {
		okChan := make(chan bool)
		go func() {
			okChan <- item.Call("KVServer.PutAppend", &args, &reply)
		}()

		select {
		case result := <-okChan:
			if result {
				return
			}
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
