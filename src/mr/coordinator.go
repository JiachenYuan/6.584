package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// This lock must be grabbed before modifying the struct
	mutex         sync.Mutex
	filenames     []string
	nMapper       int
	nReducer      int
	nMapperDone   int
	nReducerDone  int
	tasks         map[int]*Task
	mapperDoneIDs map[int]bool

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

func (c *Coordinator) GetNumReducer(args *int, reply *int) error {
	*reply = c.nReducer
	return nil
}

func (c *Coordinator) TaskLivelinessCheck() {
	for {
		c.mutex.Lock()
		keysToDelete := []int{}
		for taskID, taskInfo := range c.tasks {
			currTime := time.Now()
			timeDiff := currTime.Sub(taskInfo.status.lastSeen).Seconds()
			if timeDiff > 10 && taskInfo.status.isAssigned && !taskInfo.status.isComplete {
				// Prune and create a new task if the task has been assigned to a worker and has not been completed within 10 seconds
				keysToDelete = append(keysToDelete, taskID)
			}
		}

		for _, taskID := range keysToDelete {
			fmt.Println("has task to remove")
			taskInfo := c.tasks[taskID]
			newTask := &Task{
				id:       c.nextTaskKey,
				filename: taskInfo.filename,
				status: TaskStatus{
					isAssigned: false,
					lastSeen:   time.Now(),
					isComplete: false,
				},
				jobType: taskInfo.jobType,
			}
			c.nextTaskKey++
			c.tasks[newTask.id] = newTask
			delete(c.tasks, taskID)
		}
		c.mutex.Unlock()
		time.Sleep(time.Second)
	}
}

// ! This function assumes the caller has the coordinator has the LOCK
func (c *Coordinator) getAJob() (*Task, bool) {
	// FIXME: restrict number of mapper tasks ongoing to nMapper
	retval := &Task{}
	retval.jobType = Waiter

	if c.nReducerDone == c.nReducer {
		retval.jobType = Waiter
		return retval, true
	}
	if c.nReducerDone < c.nReducer {
		// Not all reduce jobs are done
		for _, job := range c.tasks {
			if !job.status.isAssigned {
				job.status.isAssigned = true
				job.status.lastSeen = time.Now()
				return job, true
			}
		}
	}
	return retval, false
}

// This function assumes the caller already grabs the lock of coordinator
// This function should only be called once after all mappers have finished the job
func (c *Coordinator) createReduceTasks() error {
	for i := 0; i < c.nReducer; i++ {
		// Embed information about which files the reducer should combine.
		// The format of the information could be e.g. "1,2,3,4,5-1", which means the reducer 1 should read files from mr-1-1, mr-1-2, ... , mr-5-1 and combine them
		intermediateFilesHint := ""
		for k := range c.mapperDoneIDs {
			intermediateFilesHint += fmt.Sprintf("%v,", k)
		}
		// Trim the last comma
		intermediateFilesHint = intermediateFilesHint[:len(intermediateFilesHint)-1]
		intermediateFilesHint += ("-" + strconv.Itoa(i+1))

		task := &Task{
			id:       c.nextTaskKey,
			filename: intermediateFilesHint,
			status: TaskStatus{
				isAssigned: false,
				lastSeen:   time.Now(),
				isComplete: false,
			},
			jobType: Reducer,
		}
		c.tasks[c.nextTaskKey] = task
		c.nextTaskKey++
	}
	return nil
}

// This function assumes the caller already grabs the lock of coordinator
func (c *Coordinator) createMapTasks() error {
	for _, file := range c.filenames {
		task := &Task{
			id:       c.nextTaskKey,
			filename: file,
			status: TaskStatus{
				isAssigned: false,
				lastSeen:   time.Now(),
				isComplete: false,
			},
			jobType: Mapper,
		}

		c.tasks[c.nextTaskKey] = task
		c.nextTaskKey++
	}
	return nil
}

func (c *Coordinator) HeartBeatAndGetTask(args *HeartBeatAndGetTaskArgs, reply *HeartBeatAndGetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.TaskID == 0 {
		// assign task
		task, ok := c.getAJob()
		if ok {
			reply.Filename = task.filename
			reply.JobType = task.jobType
			reply.TaskID = task.id
		} else {
			// no available job, send wait signal to worker
			reply.Filename = ""
			reply.JobType = Waiter
			reply.TaskID = 0
		}

	} else {
		// update task's last seen heartbeat time
		task, ok := c.tasks[args.TaskID]
		if !ok {
			return fmt.Errorf("[HeartBeatAndGetTask]: cannot find task %v in master's task list", args.TaskID)
		}
		task.status.lastSeen = time.Now()
	}
	// reply.Ok = true
	return nil
}

func (c *Coordinator) TaskReport(args *TaskReportArgs, reply *TaskReportReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch args.JobType {
	case Mapper:
		// Check if the reporting worker is completing a task that has not been abandoned by the coordinator
		if _, ok := c.tasks[args.TaskID]; ok {
			c.nMapperDone++
			c.tasks[args.TaskID].status.isComplete = true
			c.mapperDoneIDs[args.TaskID] = true
			// Post the reducer tasks once all files have been "mapped"
			if c.nMapperDone == len(c.filenames) {
				err := c.createReduceTasks()
				if err != nil {
					panic("[TaskReport]: cannot push reduce tasks to the queue")
				}
			}
		}
	case Reducer:
		// Check if the reporting worker is completing a task that has not been abandoned by the coordinator
		if _, ok := c.tasks[args.TaskID]; ok {
			c.nReducerDone++
			c.tasks[args.TaskID].status.isComplete = true
		}
	case Waiter:

	// case Completer:

	default:
		panic(fmt.Sprintf("[TaskReport]: unexpected jobType %v", args.JobType))
	}

	reply.Ok = true
	return nil
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
	go c.TaskLivelinessCheck()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	if c.nReducerDone == c.nReducer {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}
	c.mutex.Lock()

	// Your code here.
	// Initialize the coordinator
	c.filenames = files
	c.nMapper = 1 // ? Tunable
	c.nReducer = nReduce
	c.nMapperDone = 0
	c.nReducerDone = 0
	c.nextTaskKey = 1
	c.tasks = make(map[int]*Task)
	c.mapperDoneIDs = make(map[int]bool)

	// Fill in the task queue
	err := c.createMapTasks()
	c.mutex.Unlock()
	if err != nil {
		panic("[MakeCoordinator]: cannot push mapper tasks to the queue")
	}

	c.server()
	return &c
}
