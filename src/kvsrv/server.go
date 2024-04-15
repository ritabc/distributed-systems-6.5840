package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	db       map[string]string
	requests map[int]int64 // clientID :: requestId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	originalReqId, found := kv.requests[args.ClientId]
	if found && originalReqId == args.RequestId {
		// if this client's last request is the same as this request, basically only do a get instead
		reply.Value = kv.db[args.Key]
		return
	}
	kv.put(args.Key, args.Value)
	reply.Value = args.Value
	kv.requests[args.ClientId] = args.RequestId
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	originalReqId, found := kv.requests[args.ClientId]
	if found && originalReqId == args.RequestId {
		reply.Value = kv.db[args.Key]
		return
	}
	originalValue := kv.db[args.Key]
	newValue := originalValue + args.Value
	kv.put(args.Key, newValue)
	reply.Value = originalValue
	reply.ServerUpdated = true
	kv.requests[args.ClientId] = args.RequestId
}

func (kv *KVServer) put(k string, v string) {
	kv.db[k] = v
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.db = make(map[string]string)
	kv.requests = make(map[int]int64)

	return kv
}
