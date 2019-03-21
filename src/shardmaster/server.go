package shardmaster


import "raft"
import "labrpc"
import "labgob"
import "sync"
import "log"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}


type ShardMaster struct {
    mu      sync.Mutex
    me      int
    rf      *raft.Raft
    applyCh chan raft.ApplyMsg

    configs []Config // indexed by config num

    // Your data here.

    // Notify chan for each log index
    notifyCh map[int]chan Response
    // request records
    requests map[int32]int64 // client -> last commited reqID

    // for exit
    shutdown chan interface{}
}

// real Command
type Op struct {
    // Your data here. like union
    Operation string // join/leave/move/query

    //join
    Servers map[int][]string // new GID -> servers mappings

    //leave
    GIDs []int

    //move
    Shard int
    GID   int

    // query
    Num int // desired config number

    ID int32 // client id
    ReqID int64
}

type Response struct {
    WrongLeader bool
    Err         Err
    Config      Config

    ID int32 // client id
    ReqID int64
}

func (sm *ShardMaster) String() string {
    s := "[master_" + strconv.Itoa(sm.me) + "]:\n"
    for _, cfg := range sm.configs {
        s += cfg.String()
    }
    return s
}

// check if repeated request
func (sm *ShardMaster) isDuplicated(id int32, reqId int64) bool {
    sm.mu.Lock()
    defer sm.mu.Unlock()
    maxSeenReqId, ok := sm.requests[id]
    if ok {
        return reqId <= maxSeenReqId
    }
    return false
}

// true if update success, imply nonrepeat request can be applied to state machine: eg, data field
func (sm *ShardMaster) updateIfNotDuplicated(id int32, reqId int64) bool {
    // must hold lock outside
    maxSeenReqId, ok := sm.requests[id]
    if ok {
        if reqId <= maxSeenReqId {
            return false
        }
    }

    sm.requests[id] = reqId
    return true
}

// call raft.Start to commit a command as log entry
func (sm *ShardMaster) proposeCommand(cmd Op) (bool, *Response) {
    logIndex, _, isLeader := sm.rf.Start(cmd)
    if !isLeader {
        //DPrintf("[master %d] proposeCommand %d but not leader", sm.me, cmd.ReqID)
        return false, nil
    }

    DPrintf("[master %d] proposeCommand %d, %s, logIdx %d", sm.me, cmd.ReqID, cmd.Operation, logIndex)

    // wait command to be commited
    sm.mu.Lock()
    // use logIndex because all servers agree on same log index
    ch, ok := sm.notifyCh[logIndex]
    if !ok {
        ch = make(chan Response, 1)
        sm.notifyCh[logIndex] = ch
    }
    sm.mu.Unlock()

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
    case rsp := <-ch:
        return rsp.ID == cmd.ID && rsp.ReqID == cmd.ReqID, &rsp
        //return cmd1 == cmd // if different log, me is not leader
    }

    return false, nil
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
    // Your code here.
    reply.ID = args.ID
    reply.RspID = args.ReqID
    reply.WrongLeader = false
    reply.Err = ""

    if len(args.Servers) == 0 {
        return
    }

    for gid, _ := range args.Servers {
        if gid == 0 {
            return
        }
    }

    DPrintf("[master %d] JoinRPC args %v", sm.me, args)

    // check if repeated request, useless but efficient
    duplicate := sm.isDuplicated(args.ID, args.ReqID)
    if duplicate {
        reply.Err = ErrDuplicateReq
        return
    }

    cmd := Op{}
    cmd.Operation = "join"
    cmd.Servers = args.Servers
    cmd.ID = args.ID
    cmd.ReqID = args.ReqID

    succ, _ := sm.proposeCommand(cmd)
    if !succ {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
    }
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
    // Your code here.
    reply.ID = args.ID
    reply.RspID = args.ReqID

    if len(args.GIDs) == 0 {
        return
    }

    reply.WrongLeader = false
    reply.Err = ""

    // check if repeated request, useless but efficient
    duplicate := sm.isDuplicated(args.ID, args.ReqID)
    if duplicate {
        reply.Err = ErrDuplicateReq
        return
    }

    DPrintf("[master %d] LeaveRPC args %v", sm.me, args)

    cmd := Op{}
    cmd.Operation = "leave"
    cmd.GIDs = args.GIDs
    cmd.ID = args.ID
    cmd.ReqID = args.ReqID

    succ, _ := sm.proposeCommand(cmd)
    if !succ {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
    }
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
    // Your code here.
    reply.ID = args.ID
    reply.RspID = args.ReqID
    reply.WrongLeader = false
    reply.Err = ""

    // check if repeated request, useless but efficient
    duplicate := sm.isDuplicated(args.ID, args.ReqID)
    if duplicate {
        reply.Err = ErrDuplicateReq
        return
    }

    DPrintf("[master %d] MoveRPC args %v", sm.me, args)

    cmd := Op{}
    cmd.Operation = "move"
    cmd.Shard = args.Shard
    cmd.GID = args.GID
    cmd.ID = args.ID
    cmd.ReqID = args.ReqID

    succ, _ := sm.proposeCommand(cmd)
    if !succ {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
    }
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
    // Your code here.
    reply.WrongLeader = false
    reply.Err = ""

    cmd := Op{}
    cmd.Operation = "query"
    cmd.Num = args.Num

    succ, rsp := sm.proposeCommand(cmd)
    if !succ {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
    } else {
        reply.Config = rsp.Config
    }
}

func (sm* ShardMaster) copyLastConfig() Config {
    cfg := Config{}
    cfg.Num = sm.configs[len(sm.configs)-1].Num + 1
    cfg.Shards = sm.configs[len(sm.configs)-1].Shards
    cfg.Groups = make(map[int][]string)
    for k, v := range sm.configs[len(sm.configs)-1].Groups {
        var servers = make([]string, len(v))
        copy(servers, v)
        cfg.Groups[k] = servers
    }

    return cfg;
}

func (sm* ShardMaster) rebalance() {
    cfg := &sm.configs[len(sm.configs)-1]

    if len(cfg.Groups) == 0 {
        return
    }

    //1. make gid --> shard count
    gidShards := make(map[int]int)
    allShardsAllocated := true
    for _, gid := range cfg.Shards {
        if gid != 0 {
            gidShards[gid] += 1
        } else {
            allShardsAllocated = false
        }
    }

    minGid, min := 0, NShards + 1
    maxGid, max := 0, 0
    nGidAllocated := 0
    // 2. some gid is not allocated, set to 0
    for gid, _ := range cfg.Groups {
        if gidShards[gid] == 0 {
            gidShards[gid] += 0
        } else {
            nGidAllocated++
        }

        if gidShards[gid] > max {
            max = gidShards[gid]
            maxGid = gid
        }
        if gidShards[gid] < min {
            min = gidShards[gid]
            minGid = gid
        }
    }

    allGidAllocated := false
    if nGidAllocated == len(cfg.Groups) || nGidAllocated == NShards {
        allGidAllocated = true
    }

    if allShardsAllocated && allGidAllocated && max - min <= 1 {
        return
    }

    if allShardsAllocated {
        for shard, gid := range cfg.Shards {
            if gid == maxGid {
                cfg.Shards[shard] = minGid
                sm.rebalance()
                return
            }
        }
    } else if allGidAllocated {
        // try alloc empty shards to minGid
        for shard, gid := range cfg.Shards {
            if gid == 0 {
                cfg.Shards[shard] = minGid
                sm.rebalance()
                return
            }
        }
    } else {
        // try alloc empty shards to minGid
        for shard, gid := range cfg.Shards {
            if gid == 0 {
                cfg.Shards[shard] = minGid
                sm.rebalance()
                return
            }
        }
    }
}

// when raft commited a log entry, it'll notify me
func (sm *ShardMaster) applyRoutine() {
    for {
        var op Op
        var applyMsg raft.ApplyMsg

        select {
        case <-sm.shutdown:
            DPrintf("[master %d] shutdown applyRoutine", sm.me)
            return

        case applyMsg = <-sm.applyCh:
        }

        if !applyMsg.CommandValid {
            panic("no snapshot for ShardMaster")
        }

        op, _ = (applyMsg.Command).(Op)
        rebalance := false
        reply := Response{}
        reply.ID = op.ID
        reply.ReqID= op.ReqID

        sm.mu.Lock()
        // Follower & Leader: try apply to state machine, fail if duplicated request
        if op.Operation == "join" {
            update := sm.updateIfNotDuplicated(op.ID, op.ReqID)
            if update {
                cfg := sm.copyLastConfig()
                for k,v := range op.Servers {
                    cfg.Groups[k] = v
                }

                DPrintf("[master %d] apply for client %d Join logindex %d, new cfg %v", sm.me, op.ID, applyMsg.CommandIndex, cfg)
                sm.configs = append(sm.configs, cfg)
                rebalance = true
            }
        } else if op.Operation == "leave" {
            update := sm.updateIfNotDuplicated(op.ID, op.ReqID)
            if update {
                DPrintf("[master %d] apply for client %d Leave logindex %d", sm.me, op.ID, applyMsg.CommandIndex)
                cfg := sm.copyLastConfig()
                for _, id := range op.GIDs {
                    delete (cfg.Groups, id)
                    for shard, gid := range cfg.Shards {
                        if gid == id {
                            cfg.Shards[shard] = 0
                        }
                    }
                }

                sm.configs = append(sm.configs, cfg)
                rebalance = true
            }
        } else if op.Operation == "move" {
            update := sm.updateIfNotDuplicated(op.ID, op.ReqID)
            if update {
                DPrintf("[master %d] apply for client %d Move logindex %d", sm.me, op.ID, applyMsg.CommandIndex)
                cfg := sm.copyLastConfig()
                cfg.Shards[op.Shard] = op.GID

                sm.configs = append(sm.configs, cfg)
            }
        } else if op.Operation == "query" {
            num := len(sm.configs) - 1
            if op.Num >= 0 && op.Num < len(sm.configs) {
                num = op.Num
            }
            reply.Config = sm.configs[num]
        }

        if rebalance {
            sm.rebalance()
        }

        ch, ok := sm.notifyCh[applyMsg.CommandIndex]
        if ok {
            ch <- reply
        }

        sm.mu.Unlock()
    }
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
    sm.rf.Kill()
    // Your code here, if desired.
    close(sm.shutdown)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
    return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
    DPrintf("[master %d] StartServer", me)
    sm := new(ShardMaster)
    sm.me = me

    sm.configs = make([]Config, 1)
    sm.configs[0].Groups = map[int][]string{}

    labgob.Register(Op{})
    sm.applyCh = make(chan raft.ApplyMsg)
    sm.rf = raft.Make(servers, me, persister, sm.applyCh)

    // Your code here.
    sm.requests = make(map[int32]int64)
    sm.notifyCh = make(map[int]chan Response)
    sm.shutdown = make(chan interface{}, 1)

    go sm.applyRoutine()

    return sm
}
