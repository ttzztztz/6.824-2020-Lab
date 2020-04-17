package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type CompleteChannelObject struct {
	Type  int
	Index int
}

type Master struct {
	MapTasksStatus    map[int]int
	ReduceTasksStatus map[int]int

	Files   []string
	NReduce int

	CompleteChannel chan CompleteChannelObject

	mutex sync.Mutex
}

const TaskStatusStart = 0
const TaskStatusDoing = 1
const TaskStatusDone = 2

func (m *Master) timeOutRemove(data map[int]int, index int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	data[index] = TaskStatusStart
}

func (m *Master) timeOutHandler(taskType, taskIndex int) {
	for {
		select {
		case complete := <-m.CompleteChannel:
			if complete.Type == taskType && complete.Index == taskIndex {
				return
			}
		case <-time.After(10 * time.Second):
			if taskType == TaskTypeMap {
				m.timeOutRemove(m.MapTasksStatus, taskIndex)
			} else if taskType == TaskTypeReduce {
				m.timeOutRemove(m.ReduceTasksStatus, taskIndex)
			}
			return
		}
	}
}

func (m *Master) unsafePickOneMapTask() (bool, string, int) {
	for k, v := range m.MapTasksStatus {
		if v == TaskStatusStart {
			return true, m.Files[k], k
		}
	}

	return false, "", 0
}

func (m *Master) unsafePickOneReduceTask() (bool, int) {
	for k, v := range m.ReduceTasksStatus {
		if v == TaskStatusStart {
			return true, k
		}
	}

	return false, 0
}

func (m *Master) HandleTaskRequest(_ *TaskRequestArgs, reply *TaskRequestReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.unsafeDone() {
		reply.Type = TaskTypeFinished
		reply.FileName = ""
		return nil
	}

	if success, fileName, fileIndex := m.unsafePickOneMapTask(); success {
		reply.Type = TaskTypeMap
		reply.FileName = fileName
		reply.Index = fileIndex
		reply.Count = m.NReduce

		go m.timeOutHandler(TaskTypeMap, reply.Index)
		return nil
	}

	if !m.unsafeMapDone() {
		reply.Type = TaskTypePending
		reply.FileName = ""

		return nil
	}

	if success, fileIndex := m.unsafePickOneReduceTask(); success {
		reply.Type = TaskTypeReduce
		reply.Index = fileIndex
		reply.Count = len(m.Files)

		go m.timeOutHandler(TaskTypeReduce, reply.Index)
		return nil
	}

	reply.Type = TaskTypePending
	reply.FileName = ""
	return nil
}

func (m *Master) HandleTaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	reply.Ack = 1
	if args.Type == TaskTypeReduce {
		if m.ReduceTasksStatus[args.Index] != TaskStatusDoing {
			return nil
		}
		m.ReduceTasksStatus[args.Index] = TaskStatusDone
	} else if args.Type == TaskTypeMap {
		if m.MapTasksStatus[args.Index] != TaskStatusDoing {
			return nil
		}
		m.MapTasksStatus[args.Index] = TaskStatusDone
	}

	m.CompleteChannel <- CompleteChannelObject{
		Type:  args.Type,
		Index: args.Index,
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	_ = rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	_ = os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.unsafeDone()
}

func (m *Master) unsafeMapDone() bool {
	for _, v := range m.MapTasksStatus {
		if v != TaskStatusDone {
			return false
		}
	}

	return true
}

func (m *Master) unsafeReduceDone() bool {
	for _, v := range m.ReduceTasksStatus {
		if v != TaskStatusDone {
			return false
		}
	}

	return true
}

func (m *Master) unsafeDone() bool {
	return m.unsafeMapDone() && m.unsafeReduceDone()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		MapTasksStatus:    make(map[int]int),
		ReduceTasksStatus: make(map[int]int),
		Files:             files,
		NReduce:           nReduce,
		CompleteChannel:   make(chan CompleteChannelObject),
		mutex:             sync.Mutex{},
	}

	for k := range files {
		m.MapTasksStatus[k] = TaskStatusStart
	}
	for i := 0; i < nReduce; i++ {
		m.ReduceTasksStatus[i] = TaskStatusStart
	}

	m.server()
	return &m
}
