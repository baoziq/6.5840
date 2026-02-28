package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	NReduce  int
	MapId    int
	Type     TaskType
	FileName string
	Content  string
}

type ReportTaskArgs struct {
	Result bool
}

type ReportTaskReply struct {
	// none
}
