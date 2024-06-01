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

type TaskFinishedArgs struct {
	TYPE int
	TASK_NUM int
}

type TaskFinishedReply struct {
	Finished bool
}

type ReduceArgs struct {
}

type ReduceReply struct {
	TASK_NUM int
	N_MAP int
}

type MapArgs struct {
}

type MapReply struct {
	MAP_TASK string
	TASK_NUM int
	N_REDUCE int
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
