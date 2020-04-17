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
	Type  int
	Index int
}

type Master struct {
	MapTasksStatus    map[int]int
	ReduceTasksStatus map[int]int

	MapTaskChan    map[int]chan bool
	ReduceTaskChan map[int]chan bool

	Files   []string
	NReduce int

	mutex sync.Mutex
}

const (
	TaskStatusStart = 0
	TaskStatusDoing = 1
	TaskStatusDone  = 2
)

func (m *Master) timeOutRemove(data map[int]int, index int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	data[index] = TaskStatusStart
}

func (m *Master) timeOutHandler(taskType, taskIndex int, finishChan chan bool) {
	for {
		select {
		case complete := <-finishChan:
			if complete {
				close(finishChan)
				return
			}
		case <-time.After(10 * time.Second):
			log.Printf("[Master] time out and removed %d \n", taskIndex)
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
			log.Printf("[Master] Picked one map task %d \n", k)
			return true, m.Files[k], k
		}
	}

	return false, "", 0
}

func (m *Master) unsafePickOneReduceTask() (bool, int) {
	for k, v := range m.ReduceTasksStatus {
		if v == TaskStatusStart {
			log.Printf("[Master] Picked one reduce task %d \n", k)
			return true, k
		}
	}

	return false, 0
}

func (m *Master) HandleTaskRequest(_ *TaskRequestArgs, reply *TaskRequestReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fmt.Println(m.MapTasksStatus)
	fmt.Println(m.ReduceTasksStatus)

	if m.unsafeDone() {
		reply.Type = TaskTypeFinished
		reply.FileName = ""

		log.Printf("[Master] sent a finished task \n")
		return nil
	}

	if success, fileName, fileIndex := m.unsafePickOneMapTask(); success {
		m.MapTasksStatus[fileIndex] = TaskStatusDoing

		reply.Type = TaskTypeMap
		reply.FileName = fileName
		reply.Index = fileIndex
		reply.Count = m.NReduce

		m.MapTaskChan[fileIndex] = make(chan bool)
		go m.timeOutHandler(TaskTypeMap, reply.Index, m.MapTaskChan[fileIndex])

		log.Printf("[Master] sent a map task %d \n", reply.Index)
		return nil
	}

	if !m.unsafeMapDone() {
		reply.Type = TaskTypePending
		reply.FileName = ""

		log.Printf("[Master] sent a pending task due to not all tasks are finished\n")
		return nil
	}

	if success, fileIndex := m.unsafePickOneReduceTask(); success {
		m.ReduceTasksStatus[fileIndex] = TaskStatusDoing

		reply.Type = TaskTypeReduce
		reply.Index = fileIndex
		reply.Count = len(m.Files)

		m.ReduceTaskChan[fileIndex] = make(chan bool)
		go m.timeOutHandler(TaskTypeReduce, reply.Index, m.ReduceTaskChan[fileIndex])

		log.Printf("[Master] sent a reduce task %d \n", reply.Index)
		return nil
	}

	reply.Type = TaskTypePending
	reply.FileName = ""
	log.Printf("[Master] sent a pending task \n")
	return nil
}

func (m *Master) HandleTaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fmt.Println(m.MapTasksStatus)
	fmt.Println(m.ReduceTasksStatus)

	go func() {
		if args.Type == TaskTypeMap {
			if val, ok := m.MapTaskChan[args.Index]; ok && val != nil {
				val <- true
			}
		} else if args.Type == TaskTypeReduce {
			if val, ok := m.ReduceTaskChan[args.Index]; ok && val != nil {
				val <- true
			}
		}
	}()

	log.Printf("[Master] Ack Complete received %d %d \n", args.Index, args.Type)
	reply.Ack = 1
	if args.Type == TaskTypeReduce {
		if m.ReduceTasksStatus[args.Index] != TaskStatusDoing {
			return nil
		}
		m.ReduceTasksStatus[args.Index] = TaskStatusDone
		log.Printf("[Master] Complete Reduce: %d \n", args.Index)
	} else if args.Type == TaskTypeMap {
		if m.MapTasksStatus[args.Index] != TaskStatusDoing {
			return nil
		}
		m.MapTasksStatus[args.Index] = TaskStatusDone
		log.Printf("[Master] Complete Map: %d \n", args.Index)
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
		log.Println("listen error:", e)
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
	for k, v := range m.MapTasksStatus {
		if v != TaskStatusDone {
			log.Printf("[Master] Map not done, id: %d, status: %d \n", k, v)
			return false
		}
	}

	return true
}

func (m *Master) unsafeReduceDone() bool {
	for k, v := range m.ReduceTasksStatus {
		log.Printf("[Master] Reduce not done, id: %d, status: %d \n", k, v)
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

		MapTaskChan:    make(map[int]chan bool),
		ReduceTaskChan: make(map[int]chan bool),

		Files:           files,
		NReduce:         nReduce,
		mutex:           sync.Mutex{},
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
