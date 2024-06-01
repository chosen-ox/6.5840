package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	// "time"
)

type Coordinator struct {
	nReduce         int      // number of reduce tasks
	map_tasks       []string // list of map tasks
	is_map_assigned []atomic.Bool   // if map task is assigned
	is_map_done	 []atomic.Bool   // if map task is done
	is_reduce_assigned  []atomic.Bool   // if reduce task is assigned
	is_reduce_done  []atomic.Bool  // if reduce task is done
	map_tasks_timeouts []int
	reduce_tasks_timeouts []int

	
}

func (c *Coordinator) UpdateTask(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	if args.TYPE == 0 {
		c.is_map_done[args.TASK_NUM].Store(true) 
	} else {
		c.is_reduce_done[args.TASK_NUM].Store(true)
	}
	return nil
}



func (c *Coordinator) IsTaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	if args.TYPE == 0 {
		for i := 0; i < len(c.is_map_done); i++ {
			if !c.is_map_done[i].Load() {
				reply.Finished = false
				return nil
			}
		}
		reply.Finished = true
		return nil
	} else {
		for i := 0; i < len(c.is_reduce_done); i++ {
			if !c.is_reduce_done[i].Load() {
				reply.Finished = false
				return nil
			}
		}
		reply.Finished = true
		return nil
	}

}

func (c *Coordinator) GetNReduce(args *MapArgs, reply *MapReply) error {
	reply.N_REDUCE = c.nReduce
	return nil
}

func (c *Coordinator) GetReduceTask(args *ReduceArgs, reply *ReduceReply) error {
	reply.N_MAP = len(c.map_tasks)
	for i := 0; i < len(c.is_reduce_assigned); i++ {
		if c.is_reduce_assigned[i].CompareAndSwap(false, true) {
			reply.TASK_NUM = i
			return nil
		}
	}
	reply.TASK_NUM = -1
	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetMapTask(args *MapArgs, reply *MapReply) error {
	reply.N_REDUCE = c.nReduce

	for i := 0; i < len(c.map_tasks); i++ {
		if c.is_map_assigned[i].CompareAndSwap(false, true) {
			reply.MAP_TASK = c.map_tasks[i]
			reply.TASK_NUM = i
			return nil
		}
	}
	reply.MAP_TASK = ""

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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	map_done := true
	for i := 0; i < len(c.is_map_done); i++ {
		if !c.is_map_done[i].Load() {
			map_done = false
			break
		}
	}

	if !map_done {
		for i := 0; i < len(c.map_tasks_timeouts); i++ {
			if !c.is_map_done[i].Load() && c.is_map_assigned[i].Load() {
				if c.map_tasks_timeouts[i] > 10 {
					c.is_map_assigned[i].Store(false)
					c.map_tasks_timeouts[i] = 0
				} else {
					c.map_tasks_timeouts[i]++
				}
			}
		}
		return false
	} 

	reduce_done := true

	for i := 0; i < len(c.is_reduce_done); i++ {
		if !c.is_reduce_done[i].Load() {
			reduce_done = false
			break
		}
	}

	if !reduce_done {
		for i := 0; i < len(c.reduce_tasks_timeouts); i++ {
			if !c.is_reduce_done[i].Load() && c.is_reduce_assigned[i].Load(){
				if c.reduce_tasks_timeouts[i] > 10 {
					c.is_reduce_assigned[i].Store(false)
					c.reduce_tasks_timeouts[i] = 0
				} else {
					c.reduce_tasks_timeouts[i]++
				}
			}
		}
		return false
	}
		

	
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.map_tasks = files
	c.nReduce = nReduce

	c.is_map_assigned = make([]atomic.Bool, len(files))
	c.is_map_done = make([]atomic.Bool, len(files))
	c.map_tasks_timeouts = make([]int, len(files))

	c.is_reduce_assigned = make([]atomic.Bool, nReduce)
	c.is_reduce_done = make([]atomic.Bool, nReduce)
	c.reduce_tasks_timeouts = make([]int, nReduce)


	c.server()
	return &c
}
