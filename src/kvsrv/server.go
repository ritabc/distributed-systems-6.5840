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

type keyValue struct {
	key   string
	value string
}

type KVServer struct {
	mu sync.Mutex

	db                map[string]string
	lastClientPuts    map[int]keyValue // map of clientIds to key/value values given on puts
	lastClientAppends map[int]keyValue // same for appends
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log.Printf("Server.Get. key: %v. Value: %v\n", args.Key, kv.db[args.Key])
	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastPut, found := kv.lastClientPuts[args.ClientId]
	if found && lastPut.key == args.Key && lastPut.value == args.Value {
		// if the client's last put matches this one, basically only do a get instead
		reply.Value = kv.db[args.Key]
		return
	}
	kv.put(args.Key, args.Value)
	reply.Value = args.Value
	kv.lastClientPuts[args.ClientId] = keyValue{args.Key, args.Value}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastAppend, found := kv.lastClientAppends[args.ClientId]
	if found && lastAppend.key == args.Key && lastAppend.value == args.Value {
		reply.Value = kv.db[args.Key]
		return
	}
	originalValue := kv.db[args.Key]
	newValue := originalValue + args.Value
	kv.put(args.Key, newValue)
	reply.Value = originalValue
	reply.ServerUpdated = true
	kv.lastClientAppends[args.ClientId] = keyValue{args.Key, args.Value}
}

func (kv *KVServer) put(k string, v string) {
	kv.db[k] = v
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.db = make(map[string]string)
	kv.lastClientPuts = make(map[int]keyValue)
	kv.lastClientAppends = make(map[int]keyValue)

	return kv
}
