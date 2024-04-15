package kvsrv

type RequestType uint8

const (
	PutRequest RequestType = iota
	AppendRequest
)

// Put or Append
type PutAppendArgs struct {
	Key         string
	Value       string
	RequestType RequestType
	RequestId   int64
	ClientId    int
}

type PutAppendReply struct {
	ServerUpdated bool
	Value         string
}

type GetArgs struct {
	Key       string
	RequestId int64
	ClientId  int
}

type GetReply struct {
	Value string
}
