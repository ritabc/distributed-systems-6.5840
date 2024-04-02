package mr

import (
	"fmt"
	"log"
	"sort"
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
	mapTasksWg   sync.WaitGroup
}

type taskData struct {
	Id       int
	TaskType taskType
	Filename string
	Status   taskStatus
}

type workerData struct {
	id                    int
	currentTaskId         int
	nanoSecsAtLastRequest int
}

type ByType ([]taskData)

func (a ByType) Len() int           { return len(a) }
func (a ByType) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByType) Less(i, j int) bool { return a[j].TaskType-a[i].TaskType >= 0 }

func (c *Coordinator) RequestForAssignment(args *RequestForAssignmentArgs, reply *RequestForAssignmentReply) error {

	// Request received from worker to get new task

	// What's the next task to be assigned?
	var assigning *taskData
	c.tasksLock.Lock()
	assigning = c.firstWaitingTask() // TODO: refactor: find assiging later in this function,
	// use it's value to do stuff?

	// if there are no more waitingTasks, we're done?
	// There may still be incomplete work, but we are done with assignments
	// incorrect??? TODO assigning.Id could be -1 even if
	if assigning.Id == -1 {
		reply.NewTask.TaskType = noMoreTasks
		c.tasksLock.Unlock()
		return nil
	}
	c.tasksLock.Unlock()

	c.tasksLock.Lock()
	requestingWorkerIsNew := true
	// Is this an existing worker?
	for _, existingWorker := range c.workers {
		if args.WorkerId == existingWorker.id {
			requestingWorkerIsNew = false
			existingWorker.nanoSecsAtLastRequest = time.Now().Nanosecond()
			existingWorker.currentTaskId = assigning.Id
		}
	}
	c.tasksLock.Unlock()

	c.tasksLock.Lock()
	// Is this a new worker?
	if requestingWorkerIsNew {
		// add to c.workers
		c.workers = append(c.workers, workerData{args.WorkerId, assigning.Id, time.Now().Nanosecond()})
		c.tasksLock.Unlock()
	} else {
		// a task request is initiated from an established worker W
		// meaning the last task assigned to W has been successfully completed.
		// remove the task from c.tasks
		completedTaskId := args.CurrentTaskId
		completedTask := c.getTaskFromId(completedTaskId)
		completedTask.Status = completed

		// if completedTask was a map task,
		// decrement the wg which prevents any reduce tasks from starting before all maps are complete
		if completedTask.TaskType == mapTask {
			c.mapTasksWg.Done()
		}

		// Now check - are we done with all tasks?
		allTasksComplete := true
		for _, task := range c.tasks {
			if task.Status != completed {
				allTasksComplete = false
				break
			}
		}
		if allTasksComplete { // TODO: this block necessary?
			reply.NewTask.TaskType = noMoreTasks
			c.tasksLock.Unlock()
			return nil
		}
		c.tasksLock.Unlock()
	}

	c.tasksLock.Lock()
	if assigning.TaskType == reduceTask {
		c.tasksLock.Unlock()
		c.mapTasksWg.Wait()
		c.tasksLock.Lock()
	}

	assigning.Status = inProgress

	reply.NewTask = *assigning
	c.tasksLock.Unlock()

	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	return nil
}

func (c *Coordinator) getTaskFromId(taskId int) *taskData {
	var t *taskData
	for i := range c.tasks {
		t = &c.tasks[i]
		if t.Id == taskId {
			return t
		}
	}
	return &taskData{-1, noMoreTasks, "", notYetStarted}
}

// TODO: break into 2 methods?
// Both would need to be called under the same lock
// 1: Any timeouts? If so, edit c.tasks. get a copy of the task that was being edited by the worker (
// timedOutTaskData). Also get timedOutTask, a pointer to task in c.tasks with id == timedOutTaskData.ID.
// ID, taskType, filename can stay the same. change status to notYetStarted (
// presumably from inProgress <- debug statement here. Should not reach on print iff status is !inProgress
// Also, in this method: sort c.tasks. firstWaitingTask assumes all map tasks are first in the queue,
// before reduceTasks, before noMoreTasks
// Find the next task for assigning.
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

	// Did any of c's workers last request something > 10 seconds ago?
	for i := range c.workers {
		worker := &c.workers[i]
		workersTask := c.getTaskFromId(worker.currentTaskId)
		if workersTask.Id != -1 {
			if time.Now().Nanosecond()-worker.nanoSecsAtLastRequest > (int)(time.Second*10) && workersTask.
				Status != completed {
				fmt.Printf("worker %v timed out\n", worker.id)
				// if so, reset its status, add back to the queue
				workersTask.Status = notYetStarted
				// Also reset worker
				worker.nanoSecsAtLastRequest = time.Now().Nanosecond()
				worker.currentTaskId = -1
			}
		}
	}

	// Its now possible, after reclaiming unfinished tasks from stale workers,
	//that our tasks queue will not produce all map tasks before any reduce tasks
	// Sort the tasks slice, putting mapTasks first
	sort.Sort(ByType(c.tasks))

	for i := range c.tasks {
		task = &(c.tasks)[i] // TODO: PRECEDENCE??? are the parens necessary?
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
	c.tasksLock.Lock()
	for idx := range c.tasks {
		task := &c.tasks[idx]
		if task.Status != completed {
			c.tasksLock.Unlock()
			return false
		}
	}
	c.tasksLock.Unlock()
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// TODO: need to lock this function???
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

	c.mapTasksWg.Add(split_count + 1)
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
