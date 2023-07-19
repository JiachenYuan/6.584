package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var NumReducer int

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WordCount struct {
	Key   string `json:"key"`
	Value string    `json:"value"`
}

// for sorting by key.
type WordCountSlice []WordCount

// for sorting by key.
func (a WordCountSlice) Len() int           { return len(a) }
func (a WordCountSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a WordCountSlice) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doHeartBeat(taskID int) HeartBeatAndGetTaskReply {
	args := HeartBeatAndGetTaskArgs{}
	args.TaskID = taskID

	reply := HeartBeatAndGetTaskReply{}

	ok := call("Coordinator.HeartBeatAndGetTask", &args, &reply)
	if !ok {
		log.Printf("[doHeartBeat]: heartbeat failed! Could be that the coordinator exited after all task completed\n")
		// panic("[doHeartBeat]: heartbeat failed!")
		os.Exit(0)
	}

	// log.Printf("[doHeartBeat]: worker got heartbeat reply: %v\n", reply)

	return reply

}

func doMapper(taskInfo HeartBeatAndGetTaskReply, mapf func(string, string) []KeyValue) {
	// taskInfo.Filename
	// Final intermediate file name should be mr-X-Y, where X is the mapper task number and Y is the reducer task number
	filename := taskInfo.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	tempFiles := []*os.File{}
	jsonEncoders := []*json.Encoder{}

	for i := 1; i <= NumReducer; i++ {
		tempFilenamePattern := fmt.Sprintf("mr-%v-%v-*", taskInfo.TaskID, i)
		fd, err := os.CreateTemp("", tempFilenamePattern)
		if err != nil {
			panic("[doMapper]: Cannot create temporary files")
		}
		// Clean up the temp files after return
		defer os.Remove(fd.Name())
		tempFiles = append(tempFiles, fd)
		jsonEncoders = append(jsonEncoders, json.NewEncoder(fd))
	}

	for _, kv := range kva {
		tempFileNum := ihash(kv.Key) % NumReducer
		// parsedCnt, err := strconv.ParseInt(kv.Value, 10, 8)
		if err != nil {
			panic("[doMapper]: cannot parse count to int")
		}

		jsonEncoders[tempFileNum].Encode(WordCount(kv))
	}

	for _, file := range tempFiles {
		originalPath := file.Name()
		// Extract dir and filename from the path to the file
		_, f := filepath.Split(originalPath)
		ff := f[ : strings.LastIndex(f, "-")]
		// The intermediate files should be put in current working directory
		newPath := "./" + ff
		file.Close()
		err = os.Rename(originalPath, newPath)
		if err != nil {
			panic("[doMapper]: cannot rename a temporary file to intermediate file")
		}

	}

	// Call coordinator to siginify mapper task completion
	args := TaskReportArgs{}
	args.JobType = Mapper
	args.TaskID = taskInfo.TaskID

	reply := TaskReportReply{}

	ok := call("Coordinator.TaskReport", &args, &reply)
	if !ok || !reply.Ok {
		panic("[doMapper]: failed to send task completion report to coordinator")
	}
}

func doReducer(taskInfo HeartBeatAndGetTaskReply, reducef func(string, []string) string) {
	// Get all partitions assigned to this reducer
	intermediateFilesHint := taskInfo.Filename
	reducerIDDelimiter := strings.Index(intermediateFilesHint, "-")
	reducerID := intermediateFilesHint[reducerIDDelimiter+1:]
	mapperIDs := strings.Split(string(intermediateFilesHint[0:reducerIDDelimiter]), ",")
	ifiles := make([]string, 0, len(mapperIDs))
	for _, id := range mapperIDs {
		ifiles = append(ifiles, fmt.Sprintf("mr-%v-%v", id, reducerID))
	}
	ofileName := "mr-out-" + reducerID
	ofile, err := os.Create(ofileName)
	if err != nil {
		panic("[doReducer]: cannot create output file" + ofileName)
	}


	wc := WordCount{}
	intermediate := []WordCount{}

	for _, f := range ifiles {

		file, err := os.Open(f)
		if err != nil {
			panic(fmt.Sprintf("[doReducer]: cannot open file %v", f))
		}

		dec := json.NewDecoder(file)

		for {
			if err := dec.Decode(&wc); err != nil {
				// panic("[deReducer]: Cannot decode the wc json struct in file " + f)
				break
			}
			intermediate = append(intermediate, wc)
		}
		file.Close()
	}

	sort.Sort(WordCountSlice(intermediate))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// Call coordinator to signify reducer task completion
	args := TaskReportArgs{}
	args.JobType = Reducer 
	args.TaskID = taskInfo.TaskID

	reply := TaskReportReply{}

	ok := call("Coordinator.TaskReport", &args, &reply)
	if !ok || !reply.Ok {
		panic("[doReducer]: failed to send task completion report to coordinator")
	}	
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	
	fetchNumReducer()

	taskID := 0

	// Polling for tasks
	for {
		response := doHeartBeat(taskID)
		switch response.JobType {
		case Mapper:
			doMapper(response, mapf)
		case Reducer:
			doReducer(response, reducef)
		case Waiter:

		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}

		time.Sleep(500 * time.Millisecond)
	}

}

func fetchNumReducer() {
	placeholderArgs := 0
	ok := call("Coordinator.GetNumReducer", &placeholderArgs, &NumReducer)
	if !ok {
		panic("[Worker]: cannot fetch number of reducers from the coordinator")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
