package raftkv

import (
    "labgob"
    "labrpc"
    "log"
    "raft"
    "sync"

    "bytes"
)

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

type LastReply struct {
    ReqID int64
    Reply *GetReply
}

type KVServer struct {
    mu sync.Mutex
    me int
    // each kv server has a raft instance
    rf *raft.Raft

    // when raft commit log, it'll notify applyCh eventually
    // so we listen this applyCh
    applyCh chan raft.ApplyMsg

    maxraftstate int // snapshot if < persister.RaftStateSize()

    // Your definitions here.

    // real kv data here
    data map[string]string

    // Notify chan for each log index
    notifyCh map[int]chan Op
    // request records
    requests map[int32]int64 // client -> last commited reqID

    // for snapshot
    lastIncludedIndex int

    // for exit
    shutdown chan interface{}
}

// check if repeated request
func (kv *KVServer) isDuplicated(id int32, reqId int64) bool {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    maxSeenReqId, ok := kv.requests[id]
    if ok {
        return reqId <= maxSeenReqId
    }
    return false
}

// true if update success, imply nonrepeat request can be applied to state machine: eg, data field
func (kv *KVServer) updateIfNotDuplicated(id int32, reqId int64) bool {
    // must hold lock outside

    maxSeenReqId, ok := kv.requests[id]
    if ok {
        if reqId <= maxSeenReqId {
            return false
        }
    }

    kv.requests[id] = reqId
    return true
}

// call raft.Start to commit a command as log entry
func (kv *KVServer) proposeCommand(cmd Op) bool {
    logIndex, _, isLeader := kv.rf.Start(cmd)
    if !isLeader {
        DPrintf("[server %d] proposeCommand %d but not leader", kv.me, cmd.ReqID)
        return false
    }

    // wait command to be commited

    kv.mu.Lock()
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    // Your code here.
    // check if leader, useless but efficient
    _, isLeader := kv.rf.GetState()
    if !isLeader {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
        return
    }

    DPrintf("[server %d] GetRPC isLeader %v, args %v", kv.me, isLeader, args)
    reply.WrongLeader = false
    reply.Err = ""
    reply.ID = args.ID
    reply.RspID = args.ReqID

    cmd := Op{}
    cmd.Key = args.Key
    cmd.Value = "" // no use for Get
    cmd.Operation = "Get"
    cmd.ID = args.ID
    cmd.ReqID = args.ReqID

    // try commit cmd to raft log
    succ := kv.proposeCommand(cmd)
    if succ {
        kv.mu.Lock()
        if v, ok := kv.data[args.Key]; ok {
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    // Your code here.
    _, isLeader := kv.rf.GetState()
    DPrintf("[server %d] PutAppendRPC isLeader %v, args %v", kv.me, isLeader, args)

    reply.WrongLeader = false
    reply.Err = ""
    reply.ID = args.ID
    reply.RspID = args.ReqID

    if !isLeader {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
    } else {
        if args.Op != "Put" && args.Op != "Append" {
            reply.Err = ErrInvalidOp
            return
        }
    }

    // check if repeated request, useless but efficient
    duplicate := kv.isDuplicated(args.ID, args.ReqID)
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

    succ := kv.proposeCommand(cmd)
    if !succ {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
    }
}

// when raft commited a log entry, it'll notify me
func (kv *KVServer) applyRoutine() {
    for {
        var op Op
        var applyMsg raft.ApplyMsg

        select {
        case c := <-kv.shutdown:
            kv.shutdown <- c
            return

        case applyMsg = <-kv.applyCh:
        }

        if !applyMsg.CommandValid {
            kv.loadSnapshot(applyMsg.Snapshot)
            continue
        }

        op, _ = (applyMsg.Command).(Op)

        kv.mu.Lock()
        // Follower & Leader: try apply to state machine, fail if duplicated request
        if op.Operation == "Put" {
            update := kv.updateIfNotDuplicated(op.ID, op.ReqID)
            if update {
                DPrintf("[server %d] apply for client %d PUT key %s, value %s, logindex %d", kv.me, op.ID, op.Key, op.Value, applyMsg.CommandIndex)
                kv.data[op.Key] = op.Value
            }
        } else if op.Operation == "Append" {
            update := kv.updateIfNotDuplicated(op.ID, op.ReqID)
            if update {
                DPrintf("[server %d] apply for client %d APPEND key %s, value %s, logindex %d", kv.me, op.ID, op.Key, op.Value, applyMsg.CommandIndex)
                kv.data[op.Key] += op.Value
            }
        } else {
            // Do nothing for Get, should I cached reply?
        }

        ch, ok := kv.notifyCh[applyMsg.CommandIndex]
        if ok {
            //_, isLeader := kv.rf.GetState()
            // likely be leader
            /*
            select {
            case <-ch:
            default:
            }
            */

            ch <- op
        }

        if kv.maxraftstate >= 0 && kv.rf.RaftStateSize() >= kv.maxraftstate {
            DPrintf("(%d) state size %d", kv.me, kv.rf.RaftStateSize())
            kv.startSnapshot(applyMsg.CommandIndex)
        }

        kv.mu.Unlock()
    }
}

// for snapshot
func (kv *KVServer) startSnapshot(lastIndex int) {
    if kv.lastIncludedIndex != -1 {
        DPrintf("[server %d] startSnapshot is in progress", kv.me)
        return
    }

    DPrintf("[server %d] startSnapshot index %d with data %v", kv.me, lastIndex, kv.data)
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)

    kv.lastIncludedIndex = lastIndex

    //e.Encode(kv.lastIncludedIndex)
    e.Encode(kv.data)
    e.Encode(kv.requests)

    data := w.Bytes()
    kv.rf.StartSnapshot(data, lastIndex)
    kv.lastIncludedIndex = -1
}

func (kv *KVServer) loadSnapshot(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)

    kv.mu.Lock()
    defer kv.mu.Unlock()
    kv.data = make(map[string]string)
    kv.requests = make(map[int32]int64)

    //d.Decode(&kv.lastIncludedIndex)
    d.Decode(&kv.data)
    d.Decode(&kv.requests)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
    kv.rf.Kill()
    // Your code here, if desired.
    kv.shutdown <- 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
    // call labgob.Register on structures you want
    // Go's RPC library to marshall/unmarshall.
    labgob.Register(Op{})

    kv := new(KVServer)
    kv.me = me
    kv.maxraftstate = maxraftstate

    // You may need initialization code here.
    kv.data = make(map[string]string)
    kv.requests = make(map[int32]int64)
    kv.notifyCh = make(map[int]chan Op)
    kv.shutdown = make(chan interface{}, 1)
    kv.lastIncludedIndex = -1

    kv.applyCh = make(chan raft.ApplyMsg, 10)
    kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    // You may need initialization code here.

    // how to replay the logs from rf?
    // how to determing the commitIndex now???

    go kv.applyRoutine() // listen on applyCh, apply op to state machine

    return kv
}
