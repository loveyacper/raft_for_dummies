package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "labrpc"
import "crypto/rand"
import "math/big"
import "shardmaster"
import "time"
import "sync/atomic"

var clientIdGen = int32(0)

const maxTry = 3

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
    shard := 0
    if len(key) > 0 {
        shard = int(key[0])
    }
    shard %= shardmaster.NShards
    return shard
}

func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}

type Clerk struct {
    sm       *shardmaster.Clerk // client to shardMaster
    config   shardmaster.Config
    make_end func(string) *labrpc.ClientEnd
    // You will have to modify this struct.

    fail int // successive fail calls for leader
    clientId int32 // client id, init by clientIdGen
    reqId int64 // req id
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
    ck := new(Clerk)
    ck.sm = shardmaster.MakeClerk(masters)
    ck.make_end = make_end
    // You'll have to add code here.
    ck.clientId = atomic.AddInt32(&clientIdGen, 1)
    ck.reqId = 1

    // init config first
    ck.config = ck.sm.Query(-1)
    return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
    args := GetArgs{}
    args.Key = key
    args.ReqID = ck.reqId
    ck.reqId++
    args.ID = ck.clientId

    shard := key2shard(key)
    for {
        gid := ck.config.Shards[shard]
        if servers, ok := ck.config.Groups[gid]; ok {
            // try each server for the shard.
            for si := 0; si < len(servers); si++ {
                srv := ck.make_end(servers[si])

                // start rpc call to server
                var reply GetReply
                done := make(chan bool)
                go func() {
                    ok := srv.Call("ShardKV.Get", &args, &reply)
                    done <- ok
                }()

                // wait rpc response
                ok := true
                timeout := false
                select {
                case <-time.After(RpcTimeout):
                    timeout = true

                case ok = <-done:
                }
                //close(done)

                if !timeout && ok && !reply.WrongLeader && (reply.Err == "" || reply.Err == OK || reply.Err == ErrNoKey) {
                    DPrintf("[client %d] succ GET: %s = %s", ck.clientId, key, reply.Value)
                    return reply.Value
                }

                if ok && reply.Err == ErrWrongGroup {
                    break
                }

                // not leader or timeout, try next server in this replica
            }
        }

        time.Sleep(100 * time.Millisecond)
        // ask master for the latest configuration.
        ck.config = ck.sm.Query(-1)
    }

    return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    args := PutAppendArgs{}
    args.Key = key
    args.Value = value
    args.Op = op

    args.ReqID = ck.reqId
    ck.reqId++
    args.ID = ck.clientId

    shard := key2shard(key)
    for {
        gid := ck.config.Shards[shard]
        if servers, ok := ck.config.Groups[gid]; ok {
            // try each server for the shard.
            for si := 0; si < len(servers); si++ {
                srv := ck.make_end(servers[si])

                // start rpc call to server
                var reply PutAppendReply
                done := make(chan bool)
                go func() {
                    ok := srv.Call("ShardKV.PutAppend", &args, &reply)
                    done <- ok
                }()

                // wait rpc response
                ok := true
                timeout := false
                select {
                case <-time.After(RpcTimeout):
                    timeout = true

                case ok = <-done:
                }
                //close(done)

                if !timeout && ok && !reply.WrongLeader && (reply.Err == "" || reply.Err == OK) {
                    DPrintf("[client %d] succ PutAppend: %s = %s, %v", ck.clientId, key, value, reply.Err)
                    return
                }
                if ok && reply.Err == ErrWrongGroup {
                    break
                }
                // not leader or timeout, try next server in this replica
            }
        }

        time.Sleep(100 * time.Millisecond)
        // ask master for the latest configuration.
        ck.config = ck.sm.Query(-1)
    }
}

func (ck *Clerk) Put(key string, value string) {
    ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
    ck.PutAppend(key, value, "Append")
}

