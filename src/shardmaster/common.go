package shardmaster

import "time"
import "strconv"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (cfg *Config) String() string {
    s := "Num:" + strconv.Itoa(cfg.Num) + "\n"
    s += "shard->gid:\n"
    for shard, gid := range cfg.Shards {
        s += strconv.Itoa(shard) + "->" + strconv.Itoa(gid) + "\n"
    }
    s += "gid->nservers:\n"
    for gid, ss := range cfg.Groups {
        s += strconv.Itoa(gid) + " with nservers " + strconv.Itoa(len(ss)) + "\n"
    }
    return s
}

const (
	OK = "OK"
    ErrNotLeader = "ErrNotLeader"
    ErrDuplicateReq = "ErrDuplicateReq"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

    ID int32 // client id
    ReqID int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err

    ID int32 // client id
    RspID int64
}

type LeaveArgs struct {
	GIDs []int

    ID int32 // client id
    ReqID int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
    ID int32 // client id
    RspID int64
}

type MoveArgs struct {
	Shard int
	GID   int

    ID int32 // client id
    ReqID int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err

    ID int32 // client id
    RspID int64
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

const RpcTimeout time.Duration = 1000 * time.Millisecond

