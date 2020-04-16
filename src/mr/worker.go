package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type MapFunction = func(string, string) []KeyValue
type ReduceFunction = func(string, []string) string

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Map(mapf MapFunction, index int, data string) {

}

func Reduce(reducef ReduceFunction) {

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf MapFunction,
	reducef ReduceFunction) {
	failTime := 0
	for {
		args := TaskRequestArgs{}
		reply := TaskRequestReply{}

		if call("Master.HandleTaskRequest", &args, &reply) {
			failTime = 0

			switch reply.Type {
			case TaskTypeFinished:
				{
					fmt.Println("[Worker] Terminate after received finished request.")
					os.Exit(0)
				}
			case TaskTypeMap:
				{

				}
			case TaskTypeReduce:
				{

				}
			case TaskTypePending:
				{
					// do nothing
				}
			}

		} else {
			failTime++
			fmt.Println("[Worker] Request Failed, Will retry after 1 seconds.")
			if failTime >= 15 {
				fmt.Println("[Worker] Error After tried 15 times to dial to master, got no response, will terminate.")
				os.Exit(0)
			}
		}

		time.Sleep(1 * time.Second)
	}
}

func callComplete(data string, taskType int) {
	args := TaskCompleteArgs{
		Type: taskType,
		Data: data,
	}
	reply := TaskCompleteReply{}
	call("Master.HandleTaskComplete", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
