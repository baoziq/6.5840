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
	Waiting
)

// Add your RPC definitions here.

type Args struct {
}

type Reply struct {
	Task     TaskType
	Filename string
	TaskId   int
	NReduce  int
	FileSize int
}

type FinishArgs struct {
	Task   TaskType
	TaskId int
}

type FinishReply struct {
}
