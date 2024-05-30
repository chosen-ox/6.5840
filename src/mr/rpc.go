package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ReduceArgs struct {
	X int
}

type ReduceReply struct {
	Y int
	TASK_NUM int
	N_MAP int
}

type MapArgs struct {
	X int
}

type MapReply struct {
	Y        int
	MAP_TASK string
	TASK_NUM int
	N_REDUCE int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
