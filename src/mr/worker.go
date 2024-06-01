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


	for !CallCoordinatorForTaskFinished(0) {
	// for {
		nReduce, tasknum, filename := CallCoordinatorForMapTask()
		if filename == "" {
			time.Sleep(time.Second)
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
			tempfile, _ := os.CreateTemp("./", fmt.Sprintf("mr-%d-%d", tasknum, i) + "-*")
			onames[i] = tempfile.Name()
		}

		for _, kv := range intermediate {
			oname := onames[ihash(kv.Key)%nReduce]

			ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

			enc := json.NewEncoder(ofile)
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write %v", oname)
			}

			ofile.Close()
		}

		for i := 0; i < nReduce; i++ {
			os.Rename(onames[i], fmt.Sprintf("mr-%d-%d", tasknum, i))
		}
		CallCoordinatorForUpateTask(tasknum, 0)
		time.Sleep(time.Second)
	}

	
	
	for !CallCoordinatorForTaskFinished(1) {
		nMap, tasknum := CallCoordinatorForReduceTask()
		if tasknum == -1 {
			time.Sleep(time.Second)
			continue
		}
		intermediate := []KeyValue{}
		for i := 0; i < nMap; i++ {
			filename := fmt.Sprintf("mr-%d-%d", i, tasknum)
			file, err := os.Open(filename)
			if err != nil {
				// log.Fatalf("cannot open %v", filename)
				continue
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
		CallCoordinatorForUpateTask(tasknum, 1)


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

	// declare a reply structure.
	reply := ReduceReply{}

	ok := call("Coordinator.GetReduceTask", &args, &reply)
	if ok {

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

	// declare a reply structure.
	reply := MapReply{}


	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {

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
