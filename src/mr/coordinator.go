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

type TaskStatus int

const (
	Idle TaskStatus = iota
	Running
	Finished
)

type MapTask struct {
	filename  string
	taskId    int
	status    TaskStatus
	startTime time.Time
}

type ReduceTask struct {
	taskId    int
	status    TaskStatus
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	fileSize int
	nReduce  int
	mu       sync.Mutex
	mTask    []MapTask
	rTask    []ReduceTask
}

func (c *Coordinator) mapFinishedSum() int {
	sum := 0
	for _, item := range c.mTask {
		if item.status == Finished {
			sum++
		}
	}
	return sum
}

func (c *Coordinator) reduceFinishedSum() int {
	sum := 0
	for _, item := range c.rTask {
		if item.status == Finished {
			sum++
		}
	}
	return sum
}

func (c *Coordinator) mapDone() bool {
	return c.mapFinishedSum() == c.fileSize
}

func (c *Coordinator) reduceDone() bool {
	return c.reduceFinishedSum() == c.nReduce
}

func (c *Coordinator) Example(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapDone() && c.reduceDone() {
		reply.Task = Finish
		return nil
	}
	if !c.mapDone() {
		for i := 0; i < c.fileSize; i++ {
			if c.mTask[i].status == Running && time.Since(c.mTask[i].startTime) > 10*time.Second {
				c.mTask[i].status = Idle
			}
			if c.mTask[i].status == Idle {
				reply.Filename = c.mTask[i].filename
				reply.TaskId = i
				reply.Task = Map
				reply.NReduce = c.nReduce
				c.mTask[i].status = Running
				c.mTask[i].startTime = time.Now()
				return nil
			}
		}
		reply.Task = Waiting
		return nil
	}
	for i := 0; i < c.nReduce; i++ {
		if c.rTask[i].status == Running && time.Since(c.rTask[i].startTime) > 10*time.Second {
			c.rTask[i].status = Idle
		}
		if c.rTask[i].status == Idle {
			reply.NReduce = c.nReduce
			reply.TaskId = i
			reply.Task = Reduce
			reply.FileSize = c.fileSize
			c.rTask[i].status = Running
			c.rTask[i].startTime = time.Now()
			return nil
		}
	}
	reply.Task = Waiting
	return nil
}

func (c *Coordinator) Finish(args *FinishArgs, reply *FinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Task == Map {
		c.mTask[args.TaskId].status = Finished
		return nil
	}
	c.rTask[args.TaskId].status = Finished
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
	return c.mapDone() && c.reduceDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.fileSize = len(files)
	c.nReduce = nReduce
	c.mTask = make([]MapTask, c.fileSize)
	for i := 0; i < c.fileSize; i++ {
		c.mTask[i].filename = files[i]
		c.mTask[i].status = Idle
		c.mTask[i].taskId = i
	}
	c.rTask = make([]ReduceTask, nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.rTask[i].status = Idle
		c.rTask[i].taskId = i
	}
	c.server(sockname)
	return &c
}
