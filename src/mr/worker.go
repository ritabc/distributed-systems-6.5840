package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduceTasks to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Coordinator will use this to identify workers
	pid := os.Getpid()

	var (
		currentTaskId int
		nextTask      taskData
		nReduceTasks  int
		nMapTasks     int
		err           error
		reduceIdx     int
	)

	currentTaskId = -1
	for reduceIdx = 0; ; {
		// To get a new task:
		err = CallRequestForAssignment(pid, currentTaskId, &nextTask, &nReduceTasks, &nMapTasks)
		if err != nil {
			return
		}

		// is  next task of type map/reduce? Or of type noMoreTasks?
		if nextTask.TaskType == mapTask {
			processMapTask(nextTask, mapf, nReduceTasks)
		} else if nextTask.TaskType == reduceTask {
			processReduceTask(reducef, nextTask.Id-nMapTasks)
			reduceIdx++
		} else if nextTask.TaskType == noMoreTasks {
			os.Exit(0)
		}

		currentTaskId = nextTask.Id
	}
}

func processMapTask(task taskData, mapf func(string, string) []KeyValue, nReduce int) {

	var (
		inputFile  *os.File
		intermFile *os.File
		content    []byte
		// intermOut is entire output, divided into nReduce byte slices
		intermOut        [][]KeyValue
		mapfResult       []KeyValue
		kv               KeyValue
		arrayOfKeyValues []KeyValue
		nReduceBucket    int
		err              error
	)

	inputFile, err = os.Open(task.Filename)
	if err != nil {
		fmt.Printf("cannot open %v.\n", task.Filename)
	}
	content, err = io.ReadAll(inputFile)
	if err != nil {
		fmt.Printf("cannot read %v.\n", inputFile.Name())
	}
	inputFile.Close()

	mapfResult = mapf(task.Filename, string(content))

	// go through all the keys, add to intermOut - a slice with length nReduce.
	// Each element has a slice of kv pairs
	intermOut = make([][]KeyValue, nReduce)
	for _, kv = range mapfResult {
		reduceBucket := ihash(kv.Key) % nReduce
		intermOut[reduceBucket] = append(intermOut[reduceBucket], kv)
	}

	// for each of the nReduce el's in intermOut, create a new file and encode the arrayOfKeyValues to them
	// Create nReduce intermediate files
	for nReduceBucket, arrayOfKeyValues = range intermOut {

		interFilename := fmt.Sprintf("./mr-%v-%v", task.Id, nReduceBucket)
		intermFile, err = os.Create(interFilename)
		if err != nil {
			fmt.Printf("cannot create %v.\n", interFilename)
		}
		enc := json.NewEncoder(intermFile)
		err = enc.Encode(arrayOfKeyValues)
		if err != nil {
			fmt.Println("cannot encode mapped key value pairs.")
		}
		err = intermFile.Close()
		if err != nil {
			fmt.Printf("cannot close file %v.\n", interFilename)
		}
	}
}

func processReduceTask(reducef func(string, []string) string, reduceTaskIdx int) {
	// Take the intermediate files named mr-*-reduceTaskIdx.
	// And load the key/value pairs into memory
	// Sort and collapse the values of duplicate keys
	//// As in: in each interm file, there will be no key dups.
	//// But across intermediate files for the same reduce task (from different map tasks)
	// Pipe each key's value array through reducef
	// output file will be mr-out-reduceWorkerId
	// it'll contain 1 line per reducef output

	var (
		intermediateData []KeyValue
	)
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Printf("error getting current directory: %v.\n", cwd)
	}
	items, err := os.ReadDir(fmt.Sprintf("%v/", cwd))
	if err != nil {
		fmt.Printf("error reading items in current directory.\n")
	}

	filenameRegexp, err := regexp.Compile(fmt.Sprintf("mr-[0-9]+-%v", reduceTaskIdx))
	if err != nil {
		fmt.Println("error compiling regexp for matching intermediate file name in reduce worker.")
	}

	// Loop over files from map workers
	// If the file is one belonging to this reduce worker,
	// read its contents into intermediateData
	for _, item := range items {
		if !item.IsDir() && filenameRegexp.MatchString(item.Name()) {
			file, err := os.Open(fmt.Sprintf("%v/%v", cwd, item.Name()))
			if err != nil {
				fmt.Printf("Cannot open %v.\n", item.Name())
			}

			dec := json.NewDecoder(file)
			for {
				var kvArray []KeyValue
				if err := dec.Decode(&kvArray); err != nil {
					break
				}
				intermediateData = append(intermediateData, kvArray...)
			}
			file.Close()
		}
	}

	oname := fmt.Sprintf("%v/mr-out-%v", cwd, reduceTaskIdx)
	ofile, err := os.Create(oname)
	if err != nil {
		fmt.Printf("cannot create %v.\n", oname)
	}

	// Now, all kv pairs that hashed to this reduce worker are in intermediateData
	// Sort and collapse duplicate keys, then run (key, values) through reducef

	sort.Sort(ByKey(intermediateData))
	i := 0
	for i < len(intermediateData) {
		j := i + 1
		for j < len(intermediateData) && intermediateData[j].Key == intermediateData[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateData[k].Value)
		}
		output := reducef(intermediateData[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediateData[i].Key, output)

		i = j
	}
	ofile.Close()
}

func CallRequestForAssignment(workerId int, completedTaskId int, newTaskData *taskData, nReduce *int,
	nMap *int) error {
	args := RequestForAssignmentArgs{workerId, completedTaskId}

	reply := RequestForAssignmentReply{}
	ok := call("Coordinator.RequestForAssignment", &args, &reply)
	if !ok {
		return errors.New("rpc failed for Coordinator.AssignInputSplit()\n")
	}

	*newTaskData = reply.NewTask
	*nReduce = reply.NReduceTasks
	*nMap = reply.NMapTasks

	return nil
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
