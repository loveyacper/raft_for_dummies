package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"

import "time"

var clientIdGen = int32(0)

const maxTry = 3

type Clerk struct {
    servers []*labrpc.ClientEnd
    // You will have to modify this struct.
    leader int // hint or probe, TODO: server no use this field
    fail int // successive fail calls for leader
    clientId int32 // client id, init by clientIdGen
    reqId int64 // req id
}

func nrand() int64 {
    // What's this??? I don't use it
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
    ck := new(Clerk)
    ck.servers = servers
    // You'll have to add code here.
    ck.leader = 0
    ck.clientId = atomic.AddInt32(&clientIdGen, 1)
    ck.reqId = 1

    return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
    // You will have to modify this function.
    args := new(GetArgs)
    args.Key = key
    args.ReqID = ck.reqId
    ck.reqId++
    args.ID = ck.clientId

    fail := 0
    for {
        reply := new(GetReply)
        var done = make(chan bool)
        go func(leader int) {
            ok := ck.servers[leader].Call("KVServer.Get", args, reply)
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
            DPrintf("[client %d] succ GET: %s = %s", ck.clientId, key, reply.Value)
            return reply.Value
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

        //time.Sleep(3 * time.Millisecond)
        DPrintf("[client %d] retry GET to another server %d\n", ck.clientId, ck.leader)
    }
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    // You will have to modify this function.
    DPrintf("[client %d] try %s %s = %s to server %d\n", ck.clientId, op, key, value, ck.leader)
    args := new(PutAppendArgs)
    args.Key = key
    args.Value = value
    args.Op = op
    args.ReqID = ck.reqId
    ck.reqId++
    args.ID = ck.clientId

    fail := 0
    for {
        reply := new(PutAppendReply)
        var done = make(chan bool)
        go func(leader int) {
            ok := ck.servers[leader].Call("KVServer.PutAppend", args, reply)
            done<-ok
        }(ck.leader)

        var ok = false
        var timeout = false
        select {
            case <-time.After(RpcTimeout):
            timeout = true

        case ok = <-done:
        }

        if !timeout && ok && !reply.WrongLeader {
            DPrintf("[client %d] succ %s %s = %s\n", ck.clientId, op, key, value)
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

        //time.Sleep(3 * time.Millisecond)
        DPrintf("[client %d] retry PUT/APPEND to another server %d\n", ck.clientId, ck.leader)
    }
}

func (ck *Clerk) Put(key string, value string) {
    ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
    ck.PutAppend(key, value, "Append")
}

