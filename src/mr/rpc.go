package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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
	ReduceId int
	MapSum   int
}

type ReportTaskArgs struct {
	Type   TaskType
	Result bool
	Id     int
}

type ReportTaskReply struct {
	// none
}
