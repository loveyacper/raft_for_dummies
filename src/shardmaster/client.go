package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync/atomic"

var clientIdGen = int32(0)

const maxTry = 3

type Clerk struct {
    servers []*labrpc.ClientEnd
    // Your data here.
    leader int // hint or probe, TODO: server no use this field
    fail int // successive fail calls for leader
    clientId int32 // client id, init by clientIdGen
    reqId int64 // req id
}

func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
    ck := new(Clerk)
    ck.servers = servers
    // Your code here.
    ck.leader = 0
    ck.clientId = atomic.AddInt32(&clientIdGen, 1)
    ck.reqId = 1
    return ck
}

func (ck *Clerk) Query(num int) Config {
    args := &QueryArgs{}
    // Your code here.
    args.Num = num
    fail := 0
    for {
        reply := new(QueryReply)
        var done = make(chan bool)
        go func(leader int) {
            ok := ck.servers[leader].Call("ShardMaster.Query", args, reply)
            done<-ok
        }(ck.leader)

        var ok = true
        var timeout = false
        select {
        case <-time.After(RpcTimeout):
            timeout = true

        case ok = <-done:
        }

        if !timeout && ok && !reply.WrongLeader {
            return reply.Config
        } else {
            fail++
            if timeout || reply.WrongLeader || fail >= maxTry {
                fail = 0
                ck.leader++
                if ck.leader >= len(ck.servers) {
                    ck.leader = 0
                }
            }
        }

        time.Sleep(50 * time.Millisecond)
        if fail == 0 {
            DPrintf("[client %d] retry QUERY to another server %d\n", ck.clientId, ck.leader)
        }
    }
}

func (ck *Clerk) Join(servers map[int][]string) {
    args := &JoinArgs{}
    // Your code here.
    args.Servers = servers
    args.ReqID = ck.reqId
    ck.reqId++
    args.ID = ck.clientId

    fail := 0
    for {
        reply := new(QueryReply)
        var done = make(chan bool)
        go func(leader int) {
            ok := ck.servers[leader].Call("ShardMaster.Join", args, reply)
            done<-ok
        }(ck.leader)

        var ok = true
        var timeout = false
        select {
        case <-time.After(RpcTimeout):
            timeout = true

        case ok = <-done:
        }

        if !timeout && ok && !reply.WrongLeader {
            return
        } else {
            fail++
            if timeout || reply.WrongLeader || fail >= maxTry {
                fail = 0
                ck.leader++
                if ck.leader >= len(ck.servers) {
                    ck.leader = 0
                }
            }
        }

        time.Sleep(50 * time.Millisecond)
        if fail == 0 {
            DPrintf("[client %d] retry QUERY to another server %d\n", ck.clientId, ck.leader)
        }
    }
}

func (ck *Clerk) Leave(gids []int) {
    args := &LeaveArgs{}
    // Your code here.
    args.GIDs = gids
    args.ReqID = ck.reqId
    ck.reqId++
    args.ID = ck.clientId

    fail := 0
    for {
        reply := new(QueryReply)
        var done = make(chan bool)
        go func(leader int) {
            ok := ck.servers[leader].Call("ShardMaster.Leave", args, reply)
            done<-ok
        }(ck.leader)

        var ok = true
        var timeout = false
        select {
        case <-time.After(RpcTimeout):
            timeout = true

        case ok = <-done:
        }

        if !timeout && ok && !reply.WrongLeader {
            return
        } else {
            fail++
            if timeout || reply.WrongLeader || fail >= maxTry {
                fail = 0
                ck.leader++
                if ck.leader >= len(ck.servers) {
                    ck.leader = 0
                }
            }
        }

        time.Sleep(50 * time.Millisecond)
        if fail == 0 {
            DPrintf("[client %d] retry QUERY to another server %d\n", ck.clientId, ck.leader)
        }
    }
}

func (ck *Clerk) Move(shard int, gid int) {
    args := &MoveArgs{}
    // Your code here.
    args.Shard = shard
    args.GID = gid
    args.ReqID = ck.reqId
    ck.reqId++
    args.ID = ck.clientId

    fail := 0
    for {
        reply := new(QueryReply)
        var done = make(chan bool)
        go func(leader int) {
            ok := ck.servers[leader].Call("ShardMaster.Move", args, reply)
            done<-ok
        }(ck.leader)

        var ok = true
        var timeout = false
        select {
        case <-time.After(RpcTimeout):
            timeout = true

        case ok = <-done:
        }

        if !timeout && ok && !reply.WrongLeader {
            return
        } else {
            fail++
            if timeout || reply.WrongLeader || fail >= maxTry {
                fail = 0
                ck.leader++
                if ck.leader >= len(ck.servers) {
                    ck.leader = 0
                }
            }
        }

        time.Sleep(50 * time.Millisecond)
        if fail == 0 {
            DPrintf("[client %d] retry QUERY to another server %d\n", ck.clientId, ck.leader)
        }
    }
}

