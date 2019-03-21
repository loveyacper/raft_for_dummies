package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "log"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}


// real Command
type Op struct {
    // Your definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
    Key       string
    Value     string
    Operation string // Get Put or Append

    //Request context
    ID    int32
    ReqID int64
}

type ShardInfo struct {
    shardID int
    // real kv data here
    data map[string]string
    // request records
    requests map[int32]int64 // client -> last commited reqID
}

type ShardKV struct {
    mu           sync.Mutex
    me           int
    rf           *raft.Raft
    applyCh      chan raft.ApplyMsg
    make_end     func(string) *labrpc.ClientEnd
    gid          int
    masters      []*labrpc.ClientEnd
    maxraftstate int // snapshot if log grows this big

    // Your definitions here.
    // shards and data
    data map[int]*ShardInfo // shard id --> ShardInfo

    sm *shardmaster.Clerk // client to shardMaster
    config shardmaster.Config
    // Notify chan for each log index
    notifyCh map[int]chan Op
    // for exit
    shutdown chan interface{}
}

// check if repeated request
func (kv *ShardKV) isDuplicated(shard int, id int32, reqId int64) bool {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    maxSeenReqId, ok := kv.data[shard].requests[id]
    if ok {
        return reqId <= maxSeenReqId
    }
    return false
}

// true if update success, imply nonrepeat request can be applied to state machine: eg, data field
func (kv *ShardKV) updateIfNotDuplicated(shard int, id int32, reqId int64) bool {
    // must hold lock outside
    maxSeenReqId, ok := kv.data[shard].requests[id]
    if ok {
        if reqId <= maxSeenReqId {
            return false
        }
    }

    kv.data[shard].requests[id] = reqId
    return true
}


// call raft.Start to commit a command as log entry
func (kv *ShardKV) proposeCommand(cmd Op) bool {
    kv.mu.Lock()
    // lock kv first, think about:
    // If no lock with rf.Start, raft maybe very quick to agree.
    // Then applyRoutine will not find notifyCh on log index,
    // proposeCommand will block on notifyCh forever.
    logIndex, _, isLeader := kv.rf.Start(cmd)
    if !isLeader {
        kv.mu.Unlock()
        return false
    }

    // wait command to be commited

    // use logIndex because all servers agree on same log index
    ch, ok := kv.notifyCh[logIndex]
    if !ok {
        ch = make(chan Op, 1)
        kv.notifyCh[logIndex] = ch
    }
    kv.mu.Unlock()

    // check
    if ch == nil {
        panic("FATAL: chan is nil")
    }

    // wait on ch forever, because:
    // If I lose leadership before commit, may be partioned
    // I can't response, so wait until partion healed.
    // Eventually a log will be commited on index, then I'm
    // awaken, but cmd1 is different from cmd, return failed
    // to client.
    // If client retry another leader when I waiting, no matter.
    select {
    case cmd1 := <-ch:
        return cmd1 == cmd // if different log, me is not leader
    }

    return false
}


func (kv* ShardKV) checkGroup(key string) bool {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    shard := key2shard(key)
    if len(kv.config.Shards) <= shard {
        return false
    }

    expectGid := kv.config.Shards[shard]
    return expectGid == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
    // Your code here.
    DPrintf("[server %d] GetRPC args %v", kv.me, args)
    reply.WrongLeader = false
    reply.Err = OK
    reply.ID = args.ID
    reply.RspID = args.ReqID

    // check if wrong group
    if !kv.checkGroup(args.Key) {
        DPrintf("[server %d] GetRPC wrong group", kv.me)
        reply.Err = ErrWrongGroup
        return
    }

    // check if leader, useless but efficient
    _, isLeader := kv.rf.GetState()
    if !isLeader {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
        return
    }

    // check if repeated request, useless but efficient
    duplicate := kv.isDuplicated(key2shard(args.Key), args.ID, args.ReqID)
    if duplicate {
        reply.Err = ErrDuplicateReq
        return
    }

    cmd := Op{}
    cmd.Key = args.Key
    cmd.Value = "" // no use for Get
    cmd.Operation = "Get"
    cmd.ID = args.ID
    cmd.ReqID = args.ReqID

    // try commit cmd to raft log
    succ := kv.proposeCommand(cmd)
    if succ {
        shard := key2shard(args.Key)
        kv.mu.Lock()
        if v, ok := kv.data[shard].data[args.Key]; ok {
            reply.Value = v
        } else {
            reply.Value = ""
            reply.Err = ErrNoKey
        }
        kv.mu.Unlock()
    } else {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
    }
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    // Your code here.
    DPrintf("[server %d] PutAppendRPC args %v", kv.me, args)
    reply.WrongLeader = false
    reply.Err = OK
    reply.ID = args.ID
    reply.RspID = args.ReqID

    // check if wrong group
    if !kv.checkGroup(args.Key) {
        reply.Err = ErrWrongGroup
        return
    }

    // check if leader, useless but efficient
    _, isLeader := kv.rf.GetState()
    if !isLeader {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
        return
    } else {
        if args.Op != "Put" && args.Op != "Append" {
            reply.Err = ErrInvalidOp
            return
        }
    }

    // check if repeated request, useless but efficient
    duplicate := kv.isDuplicated(key2shard(args.Key), args.ID, args.ReqID)
    if duplicate {
        reply.Err = ErrDuplicateReq
        return
    }

    cmd := Op{}
    cmd.Key = args.Key
    cmd.Value = args.Value
    cmd.Operation = args.Op
    cmd.ID = args.ID
    cmd.ReqID = args.ReqID

    // try commit cmd to raft log
    succ := kv.proposeCommand(cmd)
    if !succ {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
    }
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
    kv.rf.Kill()
    // Your code here, if desired.
    close(kv.shutdown)
}

// when raft commited a log entry, it'll notify me
func (kv *ShardKV) applyRoutine() {
    for {
        var op Op
        var applyMsg raft.ApplyMsg

        select {
        case <-kv.shutdown:
            DPrintf("[server %d] shutdown applyRoutine", kv.me)
            return

        case applyMsg = <-kv.applyCh:
        }

        if !applyMsg.CommandValid {
            // TODO
            //kv.loadSnapshot(applyMsg.Snapshot)
            continue
        }

        op, _ = (applyMsg.Command).(Op)

        shard := key2shard(op.Key)
        kv.mu.Lock()
        // Follower & Leader: try apply to state machine, fail if duplicated request
        if op.Operation == "Put" {
            update := kv.updateIfNotDuplicated(shard, op.ID, op.ReqID)
            if update {
                kv.data[shard].data[op.Key] = op.Value
                DPrintf("[server %d] apply for client %d PUT key %s, value %s, logindex %d", kv.me, op.ID, op.Key, op.Value, applyMsg.CommandIndex)
            }
        } else if op.Operation == "Append" {
            update := kv.updateIfNotDuplicated(shard, op.ID, op.ReqID)
            if update {
                kv.data[shard].data[op.Key] += op.Value
                DPrintf("[server %d] apply for client %d APPEND key %s, value %s, logindex %d", kv.me, op.ID, op.Key, op.Value, applyMsg.CommandIndex)
            }
        } else {
            // Do nothing for Get, should I cached reply?
            var val = ""
            if v, ok := kv.data[shard].data[op.Key]; ok {
                val = v
            }
            DPrintf("[server %d] apply for client %d GET key %s, value %s, logindex %d", kv.me, op.ID, op.Key, val, applyMsg.CommandIndex)
        }

        ch, ok := kv.notifyCh[applyMsg.CommandIndex]
        if ok {
            ch <- op
        }

        /*
        if kv.maxraftstate > 0 && kv.rf.RaftStateSize() >= kv.maxraftstate {
            DPrintf("(%d) state size %d", kv.me, kv.rf.RaftStateSize())
            // If I keep mu.Lock, the startSnapshot will use raft's lock
            // But raft's applyRoutine is keeping lock and apply msg, he will be blocking with held lock.
            //go kv.startSnapshot(applyMsg.CommandIndex)
            kv.startSnapshot(applyMsg.CommandIndex)
        }
        */

        kv.mu.Unlock()
    }
}

// for snapshot

// poll shardMaster
func (kv *ShardKV) pollConfigRoutine() {
    timer := time.After(time.Duration(1) * time.Nanosecond)
    period := time.Duration(50) * time.Millisecond
    for {
        select {
        case <-kv.shutdown:
            return

        case <-timer:
            timer = time.After(period)
        }
        kv.mu.Lock()
        kv.config = kv.sm.Query(-1)
        //DPrintf("[server %d] config %v", kv.me, kv.config)
        kv.mu.Unlock()
    }
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
    DPrintf("[server %d] StartServer", me)
    // call labgob.Register on structures you want
    // Go's RPC library to marshall/unmarshall.
    labgob.Register(Op{})

    kv := new(ShardKV)
    kv.me = me
    kv.maxraftstate = maxraftstate
    kv.make_end = make_end
    kv.gid = gid
    kv.masters = masters
    kv.sm = shardmaster.MakeClerk(kv.masters)

    // Your initialization code here.

    // Use something like this to talk to the shardmaster:
    // kv.mck = shardmaster.MakeClerk(kv.masters)

    kv.applyCh = make(chan raft.ApplyMsg)
    kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.shutdown = make(chan interface{}, 1)
    kv.notifyCh = make(map[int]chan Op)

    // init shard data
    kv.data = make(map[int]*ShardInfo)
    for i := 0; i < shardmaster.NShards; i++ {
        kv.data[i] = new(ShardInfo)
        kv.data[i].shardID  = i
        kv.data[i].data = make(map[string]string)
        kv.data[i].requests = make(map[int32]int64)
    }

    go kv.pollConfigRoutine()
    //go kv.migrateRoutine() // when config changes, MakeClerk(), and send my data to dest gid
    // add a rpc interface for recv migrate data
    go kv.applyRoutine()

    return kv
}
