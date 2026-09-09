package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func handleMap(filename string, mapf func(string, string) []KeyValue, taskId int, nReduce int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	// sort.Sort(ByKey(kva))

	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		buckets[reduceId] = append(buckets[reduceId], kv)
	}
	for reduceId, bucket := range buckets {
		intermediateFile := fmt.Sprintf("mr-%v-%v", taskId, reduceId)
		file, err = os.Create(intermediateFile)
		enc := json.NewEncoder(file)
		for _, kv := range bucket {
			enc.Encode(&kv)
		}
		file.Close()
	}
	args := FinishArgs{}
	reply := FinishReply{}
	args.Task = Map
	args.TaskId = taskId
	ok := call("Coordinator.Finish", &args, &reply)
	if !ok {
		log.Fatalf("cannot call finish")
	}
}

func handleReduce(reducef func(string, []string) string, taskId int, fileSize int) {
	var kva []KeyValue
	for mapID := 0; mapID < fileSize; mapID++ {
		name := fmt.Sprintf("mr-%d-%d", mapID, taskId)
		file, err := os.Open(name)
		if err != nil {
			log.Printf("cannot open %v\n", name)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%v", taskId)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	args := FinishArgs{}
	reply := FinishReply{}
	args.Task = Reduce
	args.TaskId = taskId
	ok := call("Coordinator.Finish", &args, &reply)
	if !ok {
		log.Fatalf("cannot call finish")
	}
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	// CallExample()
	for {
		args := Args{}
		reply := Reply{}
		ok := call("Coordinator.Example", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
			continue
		}
		if reply.Task == Map {
			handleMap(reply.Filename, mapf, reply.TaskId, reply.NReduce)
			continue
		}
		if reply.Task == Reduce {
			handleReduce(reducef, reply.TaskId, reply.FileSize)
			continue
		}
		if reply.Task == Finish {
			return
		}
		if reply.Task == Waiting {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
