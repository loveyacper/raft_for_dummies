package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
    OK            = "OK"
    ErrNoKey      = "ErrNoKey"
    ErrWrongGroup = "ErrWrongGroup"

    ErrNotLeader = "ErrNotLeader"
    ErrInvalidOp = "ErrInvalidOp"
    ErrDuplicateReq = "ErrDuplicateReq"
)

type Err string

// Put or Append
type PutAppendArgs struct {
    // You'll have to add definitions here.
    Key   string
    Value string
    Op    string // "Put" or "Append"
    // You'll have to add definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
    ID int32 // client id
    ReqID int64
}

type PutAppendReply struct {
    WrongLeader bool
    Err         Err

    ID int32
    RspID int64
}

type GetArgs struct {
    Key string
    // You'll have to add definitions here.
    ID int32
    ReqID int64
}

type GetReply struct {
    WrongLeader bool
    Err         Err
    Value       string

    ID int32
    RspID int64
}

const RpcTimeout time.Duration = 1000 * time.Millisecond

