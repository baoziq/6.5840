package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		task, ok := WorkerCall()
		if !ok {
			return
		}
		switch task.Type {
		case MapTask:
			kva := mapf(task.FileName, task.Content)
			buckets := make([][]KeyValue, task.NReduce)
			// fmt.Printf("bucket's len: %d\n", len(buckets))
			for _, kv := range kva {
				index := ihash(kv.Key) % task.NReduce
				buckets[index] = append(buckets[index], kv)
			}
			for i := 0; i < task.NReduce; i++ {
				filename := fmt.Sprintf("mr-%d-%d", task.MapId, i)
				ofile, _ := os.Create(filename)
				enc := json.NewEncoder(ofile)
				for _, kv := range buckets[i] {
					enc.Encode(&kv)
				}
				ofile.Close()
			}
			report_args := ReportTaskArgs{}
			report_reply := ReportTaskReply{}
			report_args.Type = MapTask
			report_args.Result = true
			report_args.Id = task.MapId
			WorkerReply(report_args, report_reply)

		case ReduceTask:
			kva := []KeyValue{}
			for i := 0; i < task.MapSum; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, task.ReduceId)
				ofile, _ := os.Open(filename)
				dec := json.NewDecoder(ofile)

				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))
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
				oname := fmt.Sprintf("mr-out-%d", task.ReduceId)
				ofile, _ := os.Create(oname)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			report_args := ReportTaskArgs{}
			report_reply := ReportTaskReply{}
			report_args.Type = ReduceTask
			report_args.Result = true
			report_args.Id = task.ReduceId
			WorkerReply(report_args, report_reply)
		case WaitTask:
			time.Sleep(1 * time.Second)
		case ExitTask:
			return
		}
	}
}

func WorkerCall() (RequestTaskReply, bool) {
	response_args := RequestTaskArgs{}
	response_reply := RequestTaskReply{}
	// fmt.Println("workercall start")
	ok := call("Coordinator.HandleRequest", &response_args, &response_reply)
	return response_reply, ok
}

func WorkerReply(report_args ReportTaskArgs, report_reply ReportTaskReply) bool {
	ok := call("Coordinator.HandleReport", &report_args, &report_reply)
	return ok
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
