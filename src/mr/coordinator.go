package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// This lock must be grabbed before modifying the struct
	mutex        sync.Mutex
	filenames    string
	nMapper      int
	nReducer     int
	nReducerDone int
	tasks        map[int]Task

	nextTaskKey int
}

type Task struct {
	id       int
	filename string
	status   TaskStatus
	jobType  JobType
}

type TaskStatus struct {
	isAssigned bool
	lastSeen   time.Time
	isComplete bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// ! This function assumes the caller has the coordinator has the LOCK
func (c *Coordinator) getAJob() (Task, bool) {
	if c.nReducerDone < c.nReducer {
		// Not all reduce jobs are done
		for _, job := range c.tasks {
			if !job.status.isAssigned {
				job.status.isAssigned = true
				return job, true
			}
		}
	}
	return Task{}, false
}

func (c *Coordinator) HeartBeatAndGetTask(args *HeartBeatAndGetTaskArgs, reply *HeartBeatAndGetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	retval := HeartBeatAndGetTaskReply{}

	if args.taskID == 0 {
		// assign task
		task, ok := c.getAJob()
		if ok {
			retval.filename = task.filename
			retval.jobType = task.jobType
			retval.taskID = task.id
		} else {
			// no available job, send wait signal to worker
			retval.filename = ""
			retval.jobType = Waiter
			retval.taskID = 0
		}
	} else {
		// update task's last seen heartbeat time
		task, ok := c.tasks[args.taskID]
		if !ok {
			return fmt.Errorf("[HeartBeatAndGetTask]: cannot find task %v in master's task list", args.taskID)
		}
		task.status.lastSeen = time.Now()

	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
