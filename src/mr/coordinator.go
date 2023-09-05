package mr

import (
	"container/list"
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
	mutex       sync.Mutex
	mapTasks    *list.List
	mapDone     bool
	reduceTasks *list.List
	reduceDone  bool
	timers      map[Task]int64
}

// will be called by worker to exchange tasks
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Exchange(completedTask *Task, assignedTask *Task) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if completedTask.Type != WAIT {
		log.Printf("Coordinator: received completed task %v\n", *completedTask)
	}

	// remove completed task from timers
	delete(c.timers, *completedTask)
	// check if any task has timed out
	for task, timestamp := range c.timers {
		// NOTE:
		// this only guarantees AT LEAST ONCE execution
		if time.Now().UnixNano()-timestamp > 10e9 {
			// task has timed out, eligible for reassignment
			switch task.Type {
			case MAP:
				c.mapTasks.PushBack(task)
			case REDUCE:
				c.reduceTasks.PushBack(task)
			}
			// remove task from timers
			delete(c.timers, task)
		}
		// NOTE:
		// could be optimized by breaking after first non-timeout
		// however golang map iteration order is not guaranteed
	}

	// determine if all map tasks are done
	if !c.mapDone && c.mapTasks.Len() == 0 && len(c.timers) == 0 {
		c.mapDone = true
		log.Printf("Coordinator: all map tasks done\n")
	}

	// determine if all reduce tasks are done
	if !c.reduceDone && c.reduceTasks.Len() == 0 && len(c.timers) == 0 {
		c.reduceDone = true
		log.Printf("Coordinator: all reduce tasks done\n")
	}

	if !c.mapDone && c.mapTasks.Len() > 0 {
		task := c.mapTasks.Front().Value.(Task)
		*assignedTask = Task{
			Type:     MAP,
			Filename: task.Filename,
			NReduce:  task.NReduce,
		}
		c.mapTasks.Remove(c.mapTasks.Front())
		c.timers[*assignedTask] = time.Now().UnixNano()
		log.Printf("Coordinator: assigned map task %v\n", *assignedTask)
	} else if !c.mapDone && c.mapTasks.Len() == 0 {
		*assignedTask = Task{Type: WAIT}
	} else if !c.reduceDone && c.reduceTasks.Len() > 0 {
		task := c.reduceTasks.Front().Value.(Task)
		*assignedTask = Task{
			Type: REDUCE,
			Hash: task.Hash,
		}
		c.reduceTasks.Remove(c.reduceTasks.Front())
		c.timers[*assignedTask] = time.Now().UnixNano()
		log.Printf("Coordinator: assigned reduce task %v\n", *assignedTask)
	} else if !c.reduceDone && c.reduceTasks.Len() == 0 {
		*assignedTask = Task{Type: WAIT}
	} else {
		*assignedTask = Task{Type: EXIT}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	return c.mapDone && c.reduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    list.New(),
		reduceTasks: list.New(),
		timers:      make(map[Task]int64),
	}

	// create a task for each file then send to mapTasks
	for _, filename := range files {
		task := Task{
			Type:     MAP,
			Filename: filename,
			NReduce:  nReduce,
		}
		c.mapTasks.PushBack(task)
		log.Printf("Coordinator: created map task %v\n", task)
	}

	// create a task for each reduce then send to reduceTasks
	for i := 0; i < nReduce; i++ {
		task := Task{
			Type: REDUCE,
			Hash: i,
		}
		c.reduceTasks.PushBack(task)
		log.Printf("Coordinator: created reduce task %v\n", task)
	}

	c.server()
	return &c
}
