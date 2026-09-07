package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	fileSize        int
	mapFinishSum    int
	reduceFinishSum int
	nReduce         int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *Args, reply *Reply) error {
	if c.mapFinishSum != c.fileSize {
		reply.nReduce = c.nReduce
	}
	return nil
}
func (c *Coordinator) Finish(args *FinishArgs, reply *FinishReply) error {
	if args.Task == Map {
		c.mapFinishSum++
	} else {
		c.reduceFinishSum++
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
	ret := false

	// Your code here.
	if c.reduceFinishSum == c.nReduce {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.fileSize = len(files)
	c.mapFinishSum = 0
	c.nReduce = nReduce
	c.reduceFinishSum = 0

	c.server(sockname)
	return &c
}
