package kvsrv

import (
	"crypto/rand"
	// "fmt"
	"math/big"

	"6.5840/labrpc"
)


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetNextRequestId() uint64 {
	args := RequestIdArgs{}
	reply := RequestIdReply{}

	ok := ck.server.Call("KVServer.GetNextRequestId", &args, &reply)

	if ok {
		return reply.RequestId
	} else {
		// fmt.Printf("GetNextRequestId failed\n")
	}
	return 0
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// fmt.Printf("Client Get key:%s\n", key)
	args := GetArgs{}
	args.Key = key
	reply := GetReply{}

	ok := ck.server.Call("KVServer.Get", &args, &reply)

	for !ok {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}

	return reply.Value



	// You will have to modify this function.
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// fmt.Printf("Client %s key:%s, value:%s\n", op, key, value)

	request_id := ck.GetNextRequestId()

	for request_id == 0{
		request_id = ck.GetNextRequestId()
	
	}

	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.RequestId = request_id
	
	
	reply := PutAppendReply{}

	ok := ck.server.Call("KVServer."+op, &args, &reply)

	for !ok {
		ok = ck.server.Call("KVServer."+op, &args, &reply)
	}

	return reply.Value
	
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
