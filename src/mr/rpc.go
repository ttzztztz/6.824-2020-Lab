package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	TaskTypeFinished = 1
	TaskTypeMap      = 2
	TaskTypeReduce   = 3
	TaskTypePending  = 4
)

type TaskRequestArgs struct {
}

type TaskRequestReply struct {
	Type     int
	FileName string
	Index    int
	Count    int
}

type TaskCompleteArgs struct {
	Type     int
	Index    int
}

type TaskCompleteReply struct {
	Ack int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
