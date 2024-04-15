package kvsrv

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

var clientCounter = 0

type Clerk struct {
	server   *labrpc.ClientEnd
	clientId int
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

	ck.clientId = clientCounter
	clientCounter++

	return ck
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

	var reply GetReply

	for {
		args := GetArgs{key, ck.clientId}
		success := ck.server.Call("KVServer.Get", &args, &reply)
		if success {
			break
		}
	}

	return reply.Value
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

	var (
		args  PutAppendArgs
		reply PutAppendReply
	)

	if op == "Put" {
		for {
			args = PutAppendArgs{key, value, PutRequest, ck.clientId}
			success := ck.server.Call("KVServer.Put", &args, &reply)
			if success {
				break
			}
		}
	} else if op == "Append" {
		originalVal := ck.Get(key)
		for {
			args = PutAppendArgs{key, value, AppendRequest, ck.clientId}
			success := ck.server.Call("KVServer.Append", &args, &reply)
			if success {
				break
			}
		}
		if !reply.ServerUpdated {
			return originalVal
		}
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
