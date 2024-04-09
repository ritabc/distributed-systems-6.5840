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
	tasks        []taskData
	nMapTasks    int
	nReduceTasks int
	coordLock    sync.Mutex
	mapTasksWg   sync.WaitGroup
}

type taskData struct {
	Id            int
	TaskType      taskType
	Filename      string
	Status        taskStatus
	startTime     time.Time
	currentWorker int
}

func (c *Coordinator) RequestForAssignment(args *RequestForAssignmentArgs, reply *RequestForAssignmentReply) error {
	c.coordLock.Lock()

	// Start by setting what we know
	// TODO: it'd probably make sense to make separate RPC calls in each worker to get this data (1 per worker, at the start of its life)

	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	// enter this section on a worker's subsequent requests (but not its  first). A 2nd/3rd/etc request means we can (most of the time) mark the task as complete, decrement the mapTasksWG as appropriate, and check for the entire job being complete
	if args.CompletedTaskId != -1 {
		for idx := range c.tasks {
			task := &c.tasks[idx]
			if task.Id == args.CompletedTaskId {
				// If the id of the worker making this rpc request is the one associated with this task, we mark the task complete & decrement the wg as appropriate
				if task.currentWorker == args.WorkerId {
					task.Status = completed
					// the completion of a map task means we're one step closer to being able to run reduce tasks
					if task.TaskType == mapTask {
						c.mapTasksWg.Done()
					}
					break
				}
				// Otherwise this worker was previously assumed non-responsive, and its task reassigned.
				// Don't let the original worker duplicate the wg decrement
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
			reply.NewTask = taskData{-1, noMoreTasks, "", completed, time.Time{}, -1}
			c.coordLock.Unlock()
			return nil
		}
	}

	// For both 1st and subsequent requests, get the next task
	nextTask := c.firstWaitingTask()
	coordTaskPtr := c.getTaskById(nextTask.Id)
	if coordTaskPtr != nil {
		coordTaskPtr.currentWorker = args.WorkerId
	}
	if nextTask.TaskType == reduceTask {
		// TODO: does it make sense to have a field in coord that tracks whether we can proceed with reduceTasks? That would eliminate 1 unlock/lock cycle for each call of this function
		c.coordLock.Unlock()
		c.mapTasksWg.Wait()
		c.coordLock.Lock()
	}
	reply.NewTask = nextTask
	c.coordLock.Unlock()
	return nil
}

func (c *Coordinator) firstWaitingTask() taskData {
	// TODO: probably could separate map & reduce tasks in coord struct
	// check maps first
	for c.tasksOfTypeStillRunning(mapTask) {
		for idx, _ := range c.tasks {
			task := &c.tasks[idx]
			// Do we have a (map) task that's either not yet started, or has been running for over 10 seconds?
			if task.TaskType == mapTask &&
				(task.Status == notYetStarted ||
					(task.Status == inProgress &&
						!task.startTime.IsZero() &&
						time.Since(task.startTime) > time.Second*10)) {
				//update the task's status & startTime in coordinator
				task.Status = inProgress
				task.startTime = time.Now()
				return *task
			}
		}
		// If a worker crashes before completing a task & reporting back, but fewer than 10 seconds have elapsed, other workers asking for that task will not be assigned the incomplete task, because the start time is < 10 seconds ago. To fix, sleep in while loop
		c.coordLock.Unlock()
		time.Sleep(time.Second)
		c.coordLock.Lock()
	}
	// now, same for reduce
	for c.tasksOfTypeStillRunning(reduceTask) {
		for idx, _ := range c.tasks {
			task := &c.tasks[idx]
			if task.TaskType == reduceTask &&
				(task.Status == notYetStarted ||
					(task.Status == inProgress && !task.startTime.IsZero() &&
						time.Since(task.startTime) > time.Second*10)) {
				task.Status = inProgress
				task.startTime = time.Now()
				return *task
			}
		}
		c.coordLock.Unlock()
		time.Sleep(time.Second)
		c.coordLock.Lock()
	}
	return taskData{-1, noMoreTasks, "", completed, time.Time{}, -1}
}

func (c *Coordinator) getTaskById(id int) *taskData {
	for idx, _ := range c.tasks {
		task := &c.tasks[idx]
		if task.Id == id {
			return task
		}
	}
	return nil
}

func (c *Coordinator) tasksOfTypeStillRunning(taskType taskType) bool {
	for _, task := range c.tasks {
		if task.TaskType == taskType && task.Status != completed {
			return true
		}
	}
	return false
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
	c.coordLock.Lock()
	defer c.coordLock.Unlock()
	for idx := range c.tasks {
		task := &c.tasks[idx]
		if task.Status != completed {
			return false
		}
	}
	return true
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
		task.currentWorker = -1
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
		task.currentWorker = -1
		c.tasks = append(c.tasks, task)
	}

	c.server()
	c.coordLock.Unlock()
	return &c
}
