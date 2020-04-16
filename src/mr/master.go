package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type CompleteChannelObject struct {
	TaskType int
	TaskData string
}

type Master struct {
	MapTasksStatus    map[string]int
	ReduceTasksStatus map[string]int

	Files   []string
	NReduce int

	CompleteChannel chan CompleteChannelObject

	mutex sync.Mutex
}

const TaskStatusStart = 0
const TaskStatusDoing = 1
const TaskStatusDone = 2

func (m *Master) timeOutRemove(task string, taskType int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if taskType == TaskTypeReduce {
		m.ReduceTasksStatus[task] = TaskStatusStart
	} else {
		m.MapTasksStatus[task] = TaskStatusStart
	}
}

func (m *Master) timeOutHandler(task string, taskType int) {
	for {
		select {
		case complete := <-m.CompleteChannel:
			if complete.TaskType == taskType && complete.TaskData == task {
				return
			}
		case <-time.After(10 * time.Second):
			m.timeOutRemove(task, taskType)
			return
		}
	}
}

func (m *Master) unsafePickOneTask(list map[string]int) (bool, string) {
	for k, v := range list {
		if v == TaskStatusStart {
			return true, k
		}
	}

	return false, ""
}

func (m *Master) HandleTaskRequest(_ *TaskRequestArgs, reply *TaskRequestReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.unsafeDone() {
		reply.Type = TaskTypeFinished
		reply.Data = ""
		return nil
	}

	if success, data := m.unsafePickOneTask(m.MapTasksStatus); success {
		reply.Type = TaskTypeMap
		reply.Data = data
		return nil
	}

	if success, data := m.unsafePickOneTask(m.ReduceTasksStatus); success {
		reply.Type = TaskTypeReduce
		reply.Data = data
		return nil
	}

	reply.Type = TaskTypePending
	reply.Data = ""
	return nil
}

func (m *Master) HandleTaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	reply.Ack = 1
	if args.Type == TaskTypeReduce {
		if m.ReduceTasksStatus[args.Data] != TaskStatusDoing {
			return nil
		}
		m.ReduceTasksStatus[args.Data] = TaskStatusDone
	} else if args.Type == TaskTypeMap {
		if m.MapTasksStatus[args.Data] != TaskStatusDoing {
			return nil
		}
		m.MapTasksStatus[args.Data] = TaskStatusDone
	}

	m.CompleteChannel <- CompleteChannelObject{
		TaskType: args.Type,
		TaskData: args.Data,
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

func (m *Master) unsafeDone() bool {
	for _, v := range m.MapTasksStatus {
		if v != TaskStatusDone {
			return false
		}
	}

	for _, v := range m.ReduceTasksStatus {
		if v != TaskStatusDone {
			return false
		}
	}

	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		Files:   files,
		NReduce: nReduce,
	}

	fmt.Println(files)

	m.server()
	return &m
}
