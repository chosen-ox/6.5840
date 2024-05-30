package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce         int      // number of reduce tasks
	map_tasks       []string // list of map tasks
	is_map_assigned []bool   // if map task is assigned
	is_reduce_assigned  []bool   // if reduce task is done

}

func (c *Coordinator) GetNReduce(args *MapArgs, reply *MapReply) error {
	reply.N_REDUCE = c.nReduce
	return nil
}

func (c *Coordinator) GetReduceTask(args *ReduceArgs, reply *ReduceReply) error {
	reply.N_MAP = len(c.map_tasks)
	for i := 0; i < len(c.is_reduce_assigned); i++ {
		if !c.is_reduce_assigned[i] {
			reply.TASK_NUM = i
			c.is_reduce_assigned[i] = true
			return nil
		}
	}
	reply.TASK_NUM = -1
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetMapTask(args *MapArgs, reply *MapReply) error {
	reply.N_REDUCE = c.nReduce

	for i := 0; i < len(c.map_tasks); i++ {
		if !c.is_map_assigned[i] {
			reply.MAP_TASK = c.map_tasks[i]
			reply.TASK_NUM = i
			c.is_map_assigned[i] = true
			return nil
		}
	}
	reply.MAP_TASK = ""

	reply.Y = args.X + 1
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
	ret := false
	time.Sleep(20 * time.Second)
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.map_tasks = files
	c.nReduce = nReduce

	// set all element in if_map_assigned to false
	c.is_map_assigned = make([]bool, len(files))
	c.is_reduce_assigned = make([]bool, nReduce)
	for i := 0; i < len(files); i++ {
		c.is_map_assigned[i] = false
	}

	// Your code here.

	c.server()
	return &c
}
