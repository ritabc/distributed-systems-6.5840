package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type taskStatus int
type taskType int

const (
	notYetStarted taskStatus = iota
	inProgress
	completed
)
const (
	mapTask taskType = iota
	reduceTask
	noMoreTasks
)

type Coordinator struct {
	tasksLock    sync.Mutex
	tasks        []taskData
	workers      []workerData
	nMapTasks    int
	nReduceTasks int
}

type taskData struct {
	Id       int
	TaskType taskType
	Filename string
	Status   taskStatus
}

type workerData struct {
	id                int
	currentTaskId     int
	timeOfLastRequest int
}

func (c *Coordinator) RequestForAssignment(args *RequestForAssignmentArgs, reply *RequestForAssignmentReply) error {

	// Request received from worker to get new task

	// What's the next task to be assigned?
	var assigning *taskData
	c.tasksLock.Lock()
	assigning = c.firstWaitingTask()
	c.tasksLock.Unlock()

	c.tasksLock.Lock()
	requestingWorkerIsNew := true
	// Is this an existing worker?
	for _, existingWorker := range c.workers {
		if args.WorkerId == existingWorker.id {
			requestingWorkerIsNew = false
			existingWorker.timeOfLastRequest = time.Now().Nanosecond()
			existingWorker.currentTaskId = assigning.Id
		}
	}
	c.tasksLock.Unlock()

	// Is this a new worker?
	if requestingWorkerIsNew {
		// add to c.workers
		c.tasksLock.Lock()
		c.workers = append(c.workers, workerData{args.WorkerId, assigning.Id, time.Now().Nanosecond()})
		c.tasksLock.Unlock()
	} else {
		// a task request is initiated from an established worker W
		// meaning the last task assigned to W has been successfully completed.
		// Mark that task complete, remove it from c.tasks
		c.tasksLock.Lock()
		completedTask := args.CurrentTask
		i := getTaskPos(c.tasks, completedTask.Id)
		tasksAfterRemoval := make([]taskData, 0)
		tasksAfterRemoval = append(tasksAfterRemoval, c.tasks[:i]...)
		tasksAfterRemoval = append(tasksAfterRemoval, c.tasks[i+1:]...)
		c.tasks = tasksAfterRemoval
		c.tasksLock.Unlock()

		// Now, there may be no more tasks
		if len(c.tasks) == 0 {
			reply.NewTask.TaskType = noMoreTasks
			return nil
		}
	}

	c.tasksLock.Lock()
	assigning.Status = inProgress

	reply.NewTask = *assigning
	c.tasksLock.Unlock()

	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	return nil
}

func getTaskPos(tasks []taskData, taskId int) int {
	for i, el := range tasks {
		if el.Id == taskId {
			return i
		}
	}

	return -1
}

// Find the next task for assigning.
// 0th' step is to lock the coord. defer unlock
// First check whether any of c's workers last requested something > 10s ago.
// / If so, change the task it was working on to notYetStarted
// / (make sure to return any map tasks first in this case)
// Then  resolve with the first notYetStarted task (map ones go first)
// firstWaitingTask looks through allTasks, and finds first waitingTask that has status notYetStarted.
// It sets that to inProgress before returning
func (c *Coordinator) firstWaitingTask() *taskData {
	var (
		task *taskData
	)

	for i := range c.tasks {
		task = &(c.tasks)[i]
		if task.Status == notYetStarted {
			task.Status = inProgress
			return task
		}
	}
	task = &taskData{-1, noMoreTasks, "", notYetStarted}
	return task
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

	// TODO: Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduceTasks = nReduce

	var (
		split_count int
		file        string
	)

	// Add map tasks
	// Load files from main/mrcoordinator into coordinator tasks data
	for split_count, file = range files {
		var task taskData
		task.Id = split_count
		task.TaskType = mapTask
		task.Filename = file
		task.Status = notYetStarted
		c.tasks = append(c.tasks, task)
	}

	c.nMapTasks = split_count + 1

	// Add reduce tasks
	for rTask_i := split_count + 1; rTask_i < split_count+nReduce+1; rTask_i++ {
		var task taskData
		task.Id = rTask_i
		task.TaskType = reduceTask
		task.Filename = "" // Reduce worker will gather intermediate files itself, based on its ID
		task.Status = notYetStarted
		c.tasks = append(c.tasks, task)
	}

	c.workers = make([]workerData, 0)

	c.server()
	return &c
}
