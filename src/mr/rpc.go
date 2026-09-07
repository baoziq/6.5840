package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// example to show how to declare the arguments
// and reply for an RPC.
type TaskType int

const (
	Map TaskType = iota
	Reduce
	Finish
)

// Add your RPC definitions here.

type Args struct {
}

type Reply struct {
	Task     TaskType
	Filename string
	MapNo    int
	nReduce  int
}

type FinishArgs struct {
	Task TaskType
}

type FinishReply struct {
}
