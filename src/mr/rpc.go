package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Enum of different job types
type JobType int
const (
    Mapper JobType = iota
    Reducer
    Waiter
)

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

type HeartBeatAndGetTaskArgs struct {
	// NOTE: taskID of 0 would mean not yet assigned a task
	taskID int
}

type HeartBeatAndGetTaskReply struct {
	taskID int
	jobType JobType
	filename string

}


// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
