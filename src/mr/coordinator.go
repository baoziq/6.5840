package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Status int

const (
	unknown Status = iota
	waiting
	started
	finished
)

type mapTask struct {
	filename string
	status   Status
}

type Coordinator struct {
	// Your definitions here.
	NReduce           int
	mapAllFinished    bool
	reduceAllFinished bool
	map_task          []mapTask
	reduce_task       []Status
	map_count         int
	reduce_count      int
	mu                sync.Mutex
	ret               bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) map_timeout(id int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.map_task[id].status == started {
		c.map_task[id].status = waiting
	}
}

func (c *Coordinator) reduce_timeout(id int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduce_task[id] == started {
		c.reduce_task[id] = waiting
	}
}

func (c *Coordinator) HandleRequest(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// fmt.Printf("map_count: %d\n", c.map_count)
	// fmt.Printf("len(map_task): %d\n", len(c.map_task))
	if c.map_count == len(c.map_task) {
		c.mapAllFinished = true
	}

	if c.reduce_count == len(c.reduce_task) {
		c.reduceAllFinished = true
	}

	if c.mapAllFinished && c.reduceAllFinished {
		reply.Type = ExitTask
		return nil
	}

	// 有map任务
	if !c.mapAllFinished {
		n := len(c.map_task)
		for index := 0; index < n; index++ {
			file := c.map_task[index]
			if file.status == waiting {
				c.map_task[index].status = started
				reply.FileName = file.filename
				reply.MapId = index
				reply.NReduce = c.NReduce
				reply.Type = MapTask
				reply.MapSum = len(c.map_task)
				ofile, _ := os.Open(file.filename)
				content, _ := io.ReadAll(ofile)
				ofile.Close()
				reply.Content = string(content)
				// c.map_timeout(index)
				return nil
			}
		}
		// fmt.Println("map task has distributed")
		reply.Type = WaitTask
		return nil
	}
	// mt.Println("start1 handle")
	// 分配reduce任务
	n := c.NReduce
	for index := 0; index < n; index++ {
		if c.reduce_task[index] == waiting {
			c.reduce_task[index] = started
			reply.ReduceId = index
			reply.MapSum = len(c.map_task)
			reply.Type = ReduceTask
			// c.reduce_timeout(index)
			return nil
		}
		reply.Type = WaitTask
	}
	return nil
}

func (c *Coordinator) HandleReport(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Type {
	case MapTask:
		if args.Result && c.map_task[args.Id].status == started {
			c.map_count++
			c.map_task[args.Id].status = finished
		}
	case ReduceTask:
		if args.Result && c.reduce_task[args.Id] == started {
			c.reduce_count++
			c.reduce_task[args.Id] = finished
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	if c.mapAllFinished && c.reduceAllFinished {
		c.ret = true
	}

	return c.ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// fmt.Println("start")
	fmt.Printf("len(file): %d\n", len(files))
	c.map_task = make([]mapTask, len(files))
	for index, file := range files {
		c.map_task[index].filename = file
		c.map_task[index].status = waiting
	}
	c.reduce_task = make([]Status, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduce_task[i] = waiting
	}

	c.NReduce = nReduce
	c.reduceAllFinished = false
	c.mapAllFinished = false
	c.map_count = 0
	c.reduce_count = 0
	c.ret = false
	c.server(sockname)
	return &c
}
