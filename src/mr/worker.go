package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for !CallCoordinatorForTaskFinished(0) {
	// for {
		nReduce, tasknum, filename := CallCoordinatorForMapTask()
		if filename == "" {
			fmt.Println("No task available")
			continue
		}
		intermediate := []KeyValue{}

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)

		onames := make([]string, nReduce)
		for i := 0; i < nReduce; i++ {
			onames[i] = fmt.Sprintf("mr-%d-%d", tasknum, i)
			fmt.Println(onames[i])
		}

		for _, kv := range intermediate {
			oname := onames[ihash(kv.Key)%nReduce]
			ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("cannot open %v", oname)
			}
			enc := json.NewEncoder(ofile)
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write %v", oname)
			}

			ofile.Close()
		}

		CallCoordinatorForUpateTask(tasknum, 0)
		time.Sleep(time.Second)
	}

	// time.Sleep(10*time.Second)
	
	
	for {
		nMap, tasknum := CallCoordinatorForReduceTask()
		if tasknum == -1 {
			fmt.Println("No task available")
			break
		}
		intermediate := []KeyValue{}
		for i := 0; i < nMap; i++ {
			filename := fmt.Sprintf("mr-%d-%d", i, tasknum)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
				break
				}
				intermediate = append(intermediate, kv)
			}
		}

		sort.Sort(ByKey(intermediate))

		oname := fmt.Sprintf("mr-out-%d", tasknum)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()
		time.Sleep(time.Second)


	}

}

func CallCoordinatorForUpateTask(tasknum int, typ int) bool {
	args := TaskFinishedArgs{}
	reply := TaskFinishedReply{}
	args.TYPE = typ
	args.TASK_NUM = tasknum

	ok := call("Coordinator.UpdateTask", &args, &reply)
	if ok {
		return reply.Finished
	} else {
		fmt.Printf("call failed!\n")
		return false
	}
}
func CallCoordinatorForTaskFinished(typ int) bool {
	args := TaskFinishedArgs{}
	reply := TaskFinishedReply{}
	args.TYPE = typ

	ok := call("Coordinator.IsTaskFinished", &args, &reply)
	if ok {
		return reply.Finished
	} else {
		fmt.Printf("call failed!\n")
		return false
	}
}

func CallCoordinatorForReduceTask() (int, int) {
	
	// declare an argument structure.
	args := ReduceArgs{}

	// fill in the argument(s).
	args.X = 1

	// declare a reply structure.
	reply := ReduceReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.GetTask" tells the
	// receiving server that we'd like to call
	// the GetTask() method of struct Coordinator.
	ok := call("Coordinator.GetReduceTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.REDUCE_TASK %v\n", reply.TASK_NUM)

		return reply.N_MAP, reply.TASK_NUM

	} else {
		fmt.Printf("call failed!\n")
		return 0, 0
	}

}


// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallCoordinatorForMapTask() (int, int, string) {

	// declare an argument structure.
	args := MapArgs{}

	// fill in the argument(s).
	args.X = 0

	// declare a reply structure.
	reply := MapReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.GetTask" tells the
	// receiving server that we'd like to call
	// the GetTask() method of struct Coordinator.
	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.MAP_TASK %v\n", reply.MAP_TASK)

		return reply.N_REDUCE, reply.TASK_NUM, reply.MAP_TASK

	} else {
		fmt.Printf("call failed!\n")
		return 0, 0, ""
	}

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
