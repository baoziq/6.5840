package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func handleMap(filename string, mapf func(string, string) []KeyValue, mapNo int, nReduce int) {
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
	//sort.Sort(ByKey(kva))

	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		intermediateFile := fmt.Sprintf("mr-%v-%v", mapNo, reduceId)
		file, err = os.Create(intermediateFile)
		if err != nil {
			log.Fatalf("cannot create %v", intermediateFile)
		}
		file.Close()
	}
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		intermediateFile := fmt.Sprintf("mr-%v-%v", mapNo, reduceId)
		file, err := os.Open(intermediateFile)
		if err != nil {
			log.Fatalf("cannot open mr-%v-%v", mapNo, reduceId)
		}
		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv: %v", kv)
		}
		file.Close()
	}
	args := FinishArgs{}
	reply := FinishReply{}
	args.Task = Map
	ok := call("Coordinator.Finish", args, reply)
	if !ok {
		log.Fatalf("cannot call finish")
	}
}

func handleReduce(filename string, reducef func(string, []string) string) {

	args := FinishArgs{}
	reply := FinishReply{}
	args.Task = Reduce
	ok := call("Coordinator.Finish", args, reply)
	if !ok {
		log.Fatalf("cannot call finish")
	}
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	// CallExample()
	args := Args{}
	reply := Reply{}

	for {
		ok := call("Coordinator.Example", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
			continue
		}
		if reply.Task == Map {
			handleMap(reply.Filename, mapf, reply.MapNo, reply.nReduce)
			continue
		}
		if reply.Task == Reduce {
			handleReduce(reply.Filename, reducef)
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
