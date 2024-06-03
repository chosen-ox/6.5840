package kvsrv

import (
	// "fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	rwmu sync.RWMutex // mutex to protect the key-value store
	mu sync.Mutex // mutex to protect the request_done array
	kv_store map[string]string
	request_id uint64
	requests_done []bool


	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// fmt.Printf("Get key:%s\n", args.Key)
	kv.rwmu.RLock()
	defer kv.rwmu.RUnlock()
	key := args.Key
	value, ok := kv.kv_store[key]

	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}

}

func (kv *KVServer) GetNextRequestId(args *RequestIdArgs, reply *RequestIdReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.requests_done = append(kv.requests_done, false)
	reply.RequestId = atomic.AddUint64(&kv.request_id, 1)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	
	if kv.requests_done[args.RequestId]{
		defer kv.mu.Unlock()
		return 
	}
	kv.mu.Unlock()
	// fmt.Printf("Put key:%s, value:%s\n", args.Key, args.Value)
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()
	key := args.Key
	value := args.Value
	kv.kv_store[key] = value

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.requests_done[args.RequestId] = true
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// fmt.Printf("Append key:%s, value:%s\n", args.Key, args.Value)
	kv.mu.Lock()
	if kv.requests_done[args.RequestId] {
		defer kv.mu.Unlock()
		kv.rwmu.RLock()
		defer kv.rwmu.RUnlock()
		idx := strings.LastIndex(kv.kv_store[args.Key], args.Value)
		old_value := append([]byte(kv.kv_store[args.Key][:idx]), kv.kv_store[args.Key][idx+len(args.Value):]...)

		reply.Value = string(old_value)

		return
	}
	kv.mu.Unlock()
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()

	if _, ok := kv.kv_store[args.Key]; !ok {
		kv.kv_store[args.Key] = ""
	}

	key := args.Key
	value := args.Value

	old_value := kv.kv_store[args.Key]
	kv.kv_store[key] += value
	reply.Value = old_value

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.requests_done[args.RequestId] = true


}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.kv_store = make(map[string]string)
	kv.request_id = 0
	kv.requests_done = []bool{false}


	return kv
}
