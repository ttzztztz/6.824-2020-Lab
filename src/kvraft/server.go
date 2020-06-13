package kvraft

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Command string
	Key     string
	Value   string
	Seq     int
	Cid     int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate     int // snapshot if log grows this big
	lastCommandIndex int

	// Your definitions here.
	data      map[string]string
	lastReply map[int64]*LastReply
	notify    map[int]chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// A kvserver should not complete a Get() RPC if it is not part of
	// a majority (so that it does not serve stale data).
	// A simple solution is to enter every Get()
	// (as well as each Put() and Append()) in the Raft log.

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if lastReply, ok := kv.lastReply[args.Cid]; ok && args.Seq <= lastReply.Seq {
		reply.Err = lastReply.Err
		reply.Value = lastReply.Value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := Op{
		Command: "Get",
		Key:     args.Key,
		Seq:     args.Seq,
		Cid:     args.Cid,
	}

	index, startTerm, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	okChan := make(chan bool, 1)
	kv.notify[index] = okChan
	kv.mu.Unlock()

	select {
	case <-okChan:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		curTerm, isLeader := kv.rf.GetState()
		if startTerm != curTerm || !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		if val, ok := kv.data[args.Key]; ok {
			reply.Value = val
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	case <-time.After(WaitCmdTimeOut):
		reply.Err = ErrTimeout
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("Server [%d] Put append \n", kv.me)

	if _, isLeader := kv.rf.GetState(); !isLeader {
		DPrintf("Server [%d] Wrong Leader \n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if lastReply, ok := kv.lastReply[args.Cid]; ok && args.Seq <= lastReply.Seq {
		DPrintf("Server [%d] last reply %+v \n", kv.me, lastReply)
		reply.Err = lastReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := Op{
		Command: args.Op,
		Key:     args.Key,
		Seq:     args.Seq,
		Cid:     args.Cid,
		Value:   args.Value,
	}

	DPrintf("Server [%d] cmd = %+v \n", kv.me, cmd)
	index, startTerm, isLeader := kv.rf.Start(cmd)
	DPrintf("Server [%d] raft start index=%d, startTerm=%d, isLeader=%+v \n", kv.me,
		index, startTerm, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	okChan := make(chan bool)
	kv.notify[index] = okChan
	kv.mu.Unlock()

	select {
	case <-okChan:
		curTerm, isLeader := kv.rf.GetState()
		if startTerm != curTerm || !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		reply.Err = OK
	case <-time.After(WaitCmdTimeOut):
		reply.Err = ErrTimeout
		return
	}
}

//
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

func (kv *KVServer) applyDaemon() {
	for ae := range kv.applyCh {
		if command, ok := ae.Command.(Op); ok {
			kv.mu.Lock()

			DPrintf("Server [%d] Apply channel received \n", kv.me)
			if lastReply, ok := kv.lastReply[command.Cid]; !ok || command.Seq > lastReply.Seq {
				switch command.Command {
				case "Get":
					lastReply := &LastReply{
						Seq: command.Seq,
						Err: OK,
					}

					if val, ok := kv.data[command.Key]; ok {
						lastReply.Value = val
					} else {
						lastReply.Err = ErrNoKey
					}
					kv.lastReply[command.Cid] = lastReply
				case "Put":
					lastReply := &LastReply{
						Seq: command.Seq,
						Err: OK,
					}

					kv.data[command.Key] = command.Value
					kv.lastReply[command.Cid] = lastReply
				case "Append":
					lastReply := &LastReply{
						Seq: command.Seq,
						Err: OK,
					}

					kv.data[command.Key] += command.Value
					kv.lastReply[command.Cid] = lastReply
				}

				if kv.shouldTaskSnapshot() {
					kv.WriteSnapshot()
				}
			}

			if _, ok := kv.notify[ae.CommandIndex]; ok {
				go func(channel chan bool) {
					channel <- true
					close(channel)
				}(kv.notify[ae.CommandIndex])
				delete(kv.notify, ae.CommandIndex)
			}
			kv.mu.Unlock()
		}
	}
}

//
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.data = make(map[string]string)
	kv.notify = make(map[int]chan bool)
	kv.lastReply = make(map[int64]*LastReply)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.applyDaemon()
	return kv
}

func (kv *KVServer) shouldTaskSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}

	return kv.rf.GetLogSize() >= kv.maxraftstate
}

func (kv *KVServer) unsafeSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(kv.lastCommandIndex)
	e.Encode(kv.data)

	return w.Bytes()
}

func (kv *KVServer) WriteSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data := kv.unsafeSnapshotData()
	kv.rf.PersistDataAndSnapshot(kv.lastCommandIndex, data)
}

func (kv *KVServer) ReadSnapshot() {
	snapshot := kv.rf.ReadSnapshot()
	if snapshot == nil || len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	d.Decode(&kv.lastCommandIndex)
	d.Decode(&kv.data)
}
