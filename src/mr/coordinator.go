package mr

import (
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
	tasks        []taskData
	workers      []workerData
	nMapTasks    int
	nReduceTasks int
	coordLock    sync.Mutex
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

// Requires locking/unlocking to have been called around the call to this function
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

func (c *Coordinator) RequestForAssignment(args *RequestForAssignmentArgs, reply *RequestForAssignmentReply) error {
	/*
				Input / Args
				- ID of worker making request (pid)
				- the task ID if it had one already. If this was the worker's first task, completedTaskId will be -1

				Output / Reply
				- newTask taskData
				- number of reduceTasks
				- number of map tasks

				What are things we have to do in this function?
				- Start with what we know: set values of reply.NReduceTasks, reply.NMapTasks
				- We'll know right away whether this is a worker's 1st or later task, by completedTaskId
				- If it's the first request:
				-- add new worker to c.workers with these fields:
			 	--- workerId
				--- currentTaskId = -1 TODO: fill in later
				--- time.Now
				- else if it's a 2nd/3rd/etc request
				-- check for and handle timeout, including resorting c.tasks
				-- update worker's nanoseconds field in c.workers
				-- TODO: update it's worker's currenttaskId in c.workers later
		 		-- also, since this is 2nd/3rd/etc request, we must mark the task in c.tasks as complete
				--- including decrementing from wg if that task was a map one
				-- next, since we completed a task - are all tasks complete? If so, return early

				- Only then, do we look for the next task
				-- if there are no tasks with status == notYetStarted, the called method (firstWaitingTask) returns -1
				-- if firstWaitingTask returns -1, return early after:
				--- set reply's newTask = {-1, "", noMoreTasks, completed (any status is okay)
		 		--- in c.workers, find approp worker and set its currentTaskId =  -1
				-- else, return early after:
				--- set reply's newTask fields equal to that of firstWaitingTask's return value
				--- and setting c.workers' taskId == appropriate value

				Scratch:
				Would we ever need to check for timeout on first request? AKA if completedTaskId == -1.
				No - because a worker's completedTaskId will be -1 until it calls again. If it calls again,
				that means it was successful, and therefore completedTaskId != -1
	*/

	// Start by setting what we know
	// TODO: it'd probably make sense to make separate RPC calls in each worker to get this data (1 per worker, at the start of its life)
	c.coordLock.Lock()

	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	if args.CompletedTaskId == -1 {
		// this is the first worker
		c.workers = append(c.workers, workerData{args.WorkerId, -1, time.Now().Nanosecond()})
	} else {
		// this is not the first worker
		c.handleTimeouts()

		// reset this worker's timestamp field in c.workers
		for idx := range c.workers {
			worker := &c.workers[idx]
			if worker.id == args.WorkerId {
				worker.nanoSecsAtLastRequest = time.Now().Nanosecond()
			}
			break
		}

		// mark the completedtask (since this is a 2nd+ request) as complete
		for idx := range c.tasks {
			task := &c.tasks[idx]
			if task.Id == args.CompletedTaskId {
				task.Status = completed
				// the completion of a map task means we're one step closer to being able to run reduce tasks
				if task.TaskType == mapTask {
					c.mapTasksWg.Done()
				}
				break
			}
		}

		// are all tasks now complete?
		allDone := true
		for idx := range c.tasks {
			task := &c.tasks[idx]
			if task.Status != completed {
				allDone = false
				break
			}
		}
		if allDone {
			reply.NewTask = taskData{-1, noMoreTasks, "", completed}
			c.coordLock.Unlock()
			return nil
		}
	}

	// regardless of whether this is a 1st or subsequent request, get the next task
	// Set reply.NewTask && the current worker within c.workers' currentTaskId
	nextTask := c.firstWaitingTask()
	if nextTask.TaskType == reduceTask {
		//The wait here is problematic in the following scenario: last 3 map tasks are assigned -> (
		//then) -> first of those is compelted, its worker asks for a new task (this function).
		//This function locks the coord as its first step, calls Done on the WG,
		//but that only brings its value down to 2. Then we wait here,
		//and thus deadlock as we are waiting for others to call wg.Done, and they are waiting on us to unlock below
		c.coordLock.Unlock()
		c.mapTasksWg.Wait()
		c.coordLock.Lock()
	}
	reply.NewTask = *nextTask
	for idx := range c.workers {
		worker := &c.workers[idx]
		if worker.id == args.WorkerId {
			worker.currentTaskId = nextTask.Id
			break
		}
	}
	c.coordLock.Unlock()
	return nil
}

// We've got a new request, and it's a workers 2nd/3rd/etc request. Did any of c's workers last request something > 10 seconds ago?
// Assumes data in c is locked/unlocked before/after this call
func (c *Coordinator) handleTimeouts() {
	var task *taskData
	for i := range c.workers {
		worker := &c.workers[i]
		task = c.getTaskFromId(worker.currentTaskId)
		if task.Id != -1 &&
			task.Status != completed &&
			time.Now().Nanosecond()-worker.nanoSecsAtLastRequest > (int)(time.Second*100) {

			// then reclaim the task by resetting its status
			task.Status = notYetStarted
			// Also reset worker
			worker.nanoSecsAtLastRequest = time.Now().Nanosecond()
			worker.currentTaskId = -1
		}
	}
	//	It's necessary to sort whenever a timeout happens.
	//
	// For instance, say: map assigned -> reduce assigned -> map timesOut -> reduce cannot start until all maps are done
	// ==> the wg should take care of most of this, but we could deadlock if:
	// workers running map tasks timed out,
	// then the map tasks were put in the back of the queue,
	// then workers have been assigned reduce tasks,
	// which are waiting for all map workers to complete

	sort.Sort(ByType(c.tasks))
}

func (c *Coordinator) firstWaitingTask() *taskData {
	for idx := range c.tasks {
		task := &c.tasks[idx]
		if task.Status == notYetStarted {
			task.Status = inProgress
			return task
		}
	}
	return &taskData{-1, noMoreTasks, "", completed}
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
	// TODO Locking this func up causes deadlocks. Does not locking it cause bugs? Yes - even with only 1 worker,
	// there is a race condition between the coord here and the worker's RPC call
	// TODO: using tryLock, which the go docs states is usually problematic. If so, come back to here.
	// TryLock didn't fix the issue
	if c.coordLock.TryLock() {
		defer c.coordLock.Unlock()
		for idx := range c.tasks {
			task := &c.tasks[idx]
			if task.Status != completed {
				return false
			}
		}
		return true
	}
	// return 'not done' if we someone else has the lock. Maybe not the most standard use case of TryLock,
	//but I believe this is correct. Done returns true iff it loops through c.tasks (while c is locked) and all are complete.
	// Otherwise (if any are incomplete OR someone else has the lock)
	// it returns not Done
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.coordLock.Lock()
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
	c.coordLock.Unlock()
	return &c
}
