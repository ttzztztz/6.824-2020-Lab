package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapFileName(mapIndex, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d.temp", mapIndex, reduceIndex)
}

func outputFileName(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}

func Map(mapf MapFunction, mapIndex, nReduce int, fileName string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	kva := mapf(fileName, string(content))
	bucket := make([][]KeyValue, nReduce)
	for _, v := range kva {
		inBucket := ihash(v.Key) % nReduce
		bucket[inBucket] = append(bucket[inBucket], v)
	}

	for i := 0; i < nReduce; i++ {
		tempFileName := mapFileName(mapIndex, i)
		func(outputFileName string) {
			f, err := os.OpenFile(outputFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				log.Fatalf("cannot open %v", outputFileName)
			}
			defer f.Close()

			outputJson, err := json.Marshal(bucket[i])
			if err != nil {
				log.Fatalf("cannot parse json %v", outputFileName)
			}

			_, err = f.Write(outputJson)
			if err != nil {
				log.Fatalf("cannot write file to %v", outputFileName)
			}
		}(tempFileName)
	}

	callComplete(mapIndex, TaskTypeMap)
}

func Reduce(reducef ReduceFunction, nMap, reduceIndex int) {
	outputFileName := outputFileName(reduceIndex)
	outputFile, err := os.OpenFile(outputFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatalf("cannot open %v", outputFileName)
	}
	defer outputFile.Close()

	kvMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		tmpFile := mapFileName(i, reduceIndex)
		func(fileName string) {
			f, err := os.OpenFile(fileName, os.O_RDONLY, 0755)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}
			defer f.Close()
			content, err := ioutil.ReadAll(f)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}

			data := make([]KeyValue, 0)
			if err := json.Unmarshal(content, &data); err != nil {
				log.Fatalf("cannot parse json %v", fileName)
			}

			for _, val := range data {
				kvMap[val.Key] = append(kvMap[val.Key], val.Value)
			}

		}(tmpFile)
	}

	finalOutput := make([]KeyValue, 0)

	for k, v := range kvMap {
		prod := reducef(k, v)

		finalOutput = append(finalOutput, KeyValue{
			Key:   k,
			Value: prod,
		})
	}

	sort.Sort(ByKey(finalOutput))
	for _, val := range finalOutput {
		k, v := val.Key, val.Value

		fmt.Fprintf(outputFile, "%v %v\n", k, v)
	}

	callComplete(reduceIndex, TaskTypeReduce)
}

func Worker(mapf MapFunction, reducef ReduceFunction) {
	failTime := 0
	for {
		args := TaskRequestArgs{}
		reply := TaskRequestReply{}

		if call("Master.HandleTaskRequest", &args, &reply) {
			failTime = 0

			switch reply.Type {
			case TaskTypeFinished:
				{
					log.Println("[Worker] Terminate after received finished request.")
					return
				}
			case TaskTypeMap:
				Map(mapf, reply.Index, reply.Count, reply.FileName)
			case TaskTypeReduce:
				Reduce(reducef, reply.Count, reply.Index)
			case TaskTypePending:
				{
					// do nothing
				}
			}

		} else {
			failTime++
			log.Println("[Worker] Request Failed, Will retry after 1 seconds.")
			if failTime >= 15 {
				log.Println("[Worker] Error After tried 15 times to dial to master, got no response, will terminate.")
				os.Exit(0)
			}
		}

		time.Sleep(1 * time.Second)
	}
}

func callComplete(index int, taskType int) {
	args := TaskCompleteArgs{
		Type:  taskType,
		Index: index,
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
