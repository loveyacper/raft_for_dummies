package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import "time"
import mrand "math/rand"
import "fmt"

import "sync"
import "labrpc"

import "strconv"
import "bytes"
import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
    CommandValid bool // default is true, false is for raft send snapshot msg to application
    Command      interface{}
    CommandIndex int
    Snapshot []byte // if CommandValid is false. No use for lab2, only for lab3B
}

// log entry
type LogEntry struct {
    Index int
    Term int
    Command interface{}
}

// state
type RaftState int

const (
    Follower RaftState = iota
    Candidate
    Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex          // Lock to protect shared access to this peer's state
    peers     []*labrpc.ClientEnd // RPC end points of all peers
    persister *Persister          // Object to hold this peer's persisted state
    me        int                 // this peer's index into peers[]

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // persist on all servers 
    CurrentTerm int
    VotedFor int
    Logs []LogEntry

    // volatile on all servers 
    commitIndex int
    lastApply int
    state RaftState

    // for follower or candidate
    leaderActive bool
    leader int
    electTimer *time.Timer

    heartPeriod time.Duration

    // for leader
    nextIndex []int
    matchIndex []int

    appendNotify []chan int // If len(Logs) > nextIndex[i], notify appendNotify[i]

    // for apply log to state machine
    applyCh chan ApplyMsg
    applyNotify chan interface{} // when commitIndex update

    // for snapshot
    lastIncludedIndex int
    lastIncludedTerm int

    // for exit
    shutdown chan interface{}
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    var term int
    var isleader bool
    // Your code here (2A).

    rf.mu.Lock()
    defer rf.mu.Unlock()
    term = rf.CurrentTerm
    isleader = (rf.state == Leader)
    return term, isleader
}

func (rf *Raft) GetStateWithoutLock() (int, bool) {
    var term int
    var isleader bool

    term = rf.CurrentTerm
    isleader = (rf.state == Leader)
    if isleader && rf.leader != rf.me {
        panic("Why not leader me")
    }
    return term, isleader
}

func (rf *Raft) raftStateBytes() []byte {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.CurrentTerm)
    e.Encode(rf.VotedFor)
    e.Encode(rf.Logs)
    DPrintf("[me %d]: raftStateBytes logs %v", rf.me, rf.Logs)
    return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    // Your code here (2C).
    data := rf.raftStateBytes()
    rf.persister.SaveRaftState(data)

    DPrintf("[me %d]: Persist term %d, vote %d, logs %v", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Logs)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }
    // Your code here (2C).
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)

    var term int
    var vote int
    var logs []LogEntry

    if  d.Decode(&term) != nil ||
        d.Decode(&vote) != nil ||
        d.Decode(&logs) != nil {
        DPrintf("[me %d]: readPersist failed!", rf.me)
        panic("readPersist failed")
    } else {
        rf.CurrentTerm = term
        rf.VotedFor = vote
        rf.Logs = logs
        DPrintf("[me %d]: readPersist term %d, vote %d, logs %v", rf.me, term, vote, logs)
    }
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).
    Term int
    Granted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    DPrintf("[me %d]RequestVote handle term %d, candi %d, my term %d and state %d, my leader %d",
            rf.me,
            args.Term, args.CandidateId,
            rf.CurrentTerm, int(rf.state), rf.leader)
    // Your code here (2A, 2B).
    if rf.CurrentTerm > args.Term {
        reply.Term = rf.CurrentTerm
        reply.Granted = false
    } else {
        needPersist := false
        if rf.CurrentTerm < args.Term {
            // switch to follower first
            rf.CurrentTerm = args.Term
            rf.leader = -1
            rf.leaderActive = false
            rf.VotedFor = -1 // I'm not sure vote for you now
            rf.switchToFollower()
            needPersist = true
        }

        switch rf.state {
        case Candidate:
            fallthrough
        case Follower:
            // 2B election restriction: check if newer Logs
            reply.Term = args.Term

            acceptLog := rf.checkAcceptLog(args.LastLogTerm, args.LastLogIndex)
            if acceptLog && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) {
                reply.Granted = true

                rf.CurrentTerm = args.Term
                rf.VotedFor = args.CandidateId
                needPersist = true
                // should reset elect timer
                rf.resetElectWithRandTimeout()
            } else {
                // during same term can only vote once
                // or candidate's log is not complete
                reply.Granted = false
            }

        case Leader:
            DPrintf("[me %d] I am leader, recv RequestVote same term %d, refuse it", rf.me, args.Term)
            reply.Term = rf.CurrentTerm
            reply.Granted = false
        }

        if needPersist {
            rf.persist()
        }
    }
}

func (rf* Raft) checkAcceptLog(lastLogTerm int, lastLogIndex int) bool {
    myLastIndex, myLastTerm := rf.lastLogIndexAndTerm()
    if myLastTerm < lastLogTerm {
        return true
    } else if myLastTerm > lastLogTerm {
        return false
    } else if myLastIndex <= lastLogIndex {
        return true
    } else {
        return false
    }
}

// get last log index and term
func (rf *Raft) lastLogIndexAndTerm() (int, int) {
    if len(rf.Logs) == 0 {
        return -1, -1
    } else {
        index := rf.Logs[len(rf.Logs)-1].Index
        term := rf.Logs[len(rf.Logs)-1].Term
        return index, term
    }
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}

type AppendEntriesArgs struct {
    Term int // leader's term
    LeaderId int // so follower can redirect clients

    PrevLogIndex int
    PrevLogTerm int
    Entries []LogEntry
    LeaderCommit int
}
type AppendEntriesReply struct {
     Term int // CurrentTerm, for leader to update itself
     Success bool // true if follower contained entry matching prevLogIndex/Term
     FirstIndexOfFailTerm int // if log confilict, return the first index of conflict term
}
//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    DPrintf("[me %d] AppendEntries args term %d, leader Id %d, prevIndex&Term (%d,%d), entries %d, leaderCommit %d, my term %d, state %d",
    rf.me,
    args.Term, args.LeaderId,
    args.PrevLogIndex, args.PrevLogTerm,
    len(args.Entries), args.LeaderCommit,
    rf.CurrentTerm, int(rf.state))

    if rf.CurrentTerm > args.Term {
        reply.Term = rf.CurrentTerm
        reply.Success = false
    } else {
        rf.leaderCheck(args.Term, args.LeaderId)

        needPersist := false
        if rf.CurrentTerm < args.Term {
            rf.CurrentTerm = args.Term
            needPersist = true
        }
        rf.leader = args.LeaderId
        rf.leaderActive = true
        if rf.state != Follower {
            rf.switchToFollower()
        }

        reply.Term = args.Term

        if args.PrevLogIndex < 0 {
            panic("PrevLogIndex should not < 0 because index is 1-based")
        }

        relativePrevIndex := rf.relativeIndex(args.PrevLogIndex)
        if relativePrevIndex < 0 {
            // Example: Follower's logs are [37,38,39], 1-36 is snapshot.
            // It recv append RPC before as : Prev is 39, entries are 40, 41
            // So follower's logs are [37,38,39,40,41], but the reply lost, so leader
            // still think follower's nextIndex is 40

            // then follower start another snapshot, and its logs become empty, but snapshot is 1-41
            // now we get here, leader retry append RPC, Prev is 39, entries are 40 41
            // and the relativePrevIndex will be -2, but it's legal!

            DPrintf("[me %d] relativePrevIndex %d < 0, lastIndex %d, logs %v", rf.me, relativePrevIndex, rf.lastIncludedIndex, rf.Logs)
            reply.Success = true
            //err := fmt.Sprintf("[me %d] relativePrevIndex %d < 0, lastIndex %d, logs %v", rf.me, relativePrevIndex, rf.lastIncludedIndex, rf.Logs)
            //panic(err)
            //panic("relativePrevIndex < 0 impossible because follower's log must behind leader")
        } else if len(rf.Logs) > relativePrevIndex &&
                  rf.Logs[relativePrevIndex].Term == args.PrevLogTerm {
            reply.Success = true
        } else {
            reply.Success = false
            // optimize for leader's nextIndex probe
            reply.FirstIndexOfFailTerm = rf.optimizeHintNextIndex(args.PrevLogIndex)
            DPrintf("[me %d] log conflict my len %d, args index & term %d & %d", rf.me, len(rf.Logs), args.PrevLogIndex, args.PrevLogTerm)
        }

        if reply.Success {
            // DO NOT truncate, but overwrite or append
            // If conflict, truncate!
            if rf.processLogEntry(args) {
                needPersist = true
            }

            // process commitIndex
            if args.LeaderCommit > rf.commitIndex {
                // set commitIndex to min(LeaderCommit, last new entry)
                rf.commitIndex = args.LeaderCommit
                if rf.commitIndex > args.PrevLogIndex + len(args.Entries) {
                    rf.commitIndex = args.PrevLogIndex + len(args.Entries)
                }
                DPrintf("[me %d] update commitIndex to %d, log len %d", rf.me, rf.commitIndex, len(rf.Logs))

                // apply routine
                rf.applyNotify<-0
            }
        }

        if needPersist {
            // persist even if not success, because term may changed and log is refused
            rf.persist()
        }
    }
}

func (rf* Raft) leaderCheck(term int, leaderId int) {
    // some check for same term
    if rf.CurrentTerm == term {
        if rf.state == Leader {
            err := fmt.Sprintf("[me %d]Another leader in same term %d, other %d", rf.me, term, leaderId)
            panic(err)
        } else {
            // panic if not same leader in same term?
            if rf.leader != -1 && rf.leader != leaderId {
                panic("two leaders in same term")
            }
        }
    }
}


// imply leader to probe nextIndex
func (rf* Raft) optimizeHintNextIndex(prevLogIndex int) int {
    lastIndex := rf.Logs[len(rf.Logs) - 1].Index
    if prevLogIndex > lastIndex {
        return lastIndex + 1
    } else {
        relativePrevIndex := rf.relativeIndex(prevLogIndex)
        term := rf.Logs[relativePrevIndex].Term
        i := 0
        for i = relativePrevIndex - 1; i >= 0; i-- {
            if rf.Logs[i].Term != term {
                i++
                break
            }
        }

        // log0 is the sentinel log entry so min(nextIndex) == 1
        if i < 1 {
            i = 1
        }

        DPrintf("[me %d] prevLogIndex %d, lastIndex %d, term %d, i %d logs %v", rf.me, prevLogIndex, rf.lastIncludedIndex, term, i, rf.Logs)
        return rf.absIndex(i)
    }
}

// follower process log entries from leader
func (rf* Raft) processLogEntry(args* AppendEntriesArgs) bool {
    for i, v := range args.Entries {
        idx := i + 1 + args.PrevLogIndex
        idx = rf.relativeIndex(idx)

        if idx < 0 {
            // leader may send old entries, these already be snapshot.
            continue
        }

        if len(rf.Logs) > idx {
            if rf.Logs[idx].Term != v.Term {
                // conflicit, truncate and append!
                DPrintf("[me %d] log conflict truncate up to index %d", rf.me, idx)
                rf.Logs = rf.Logs[:idx]
                rf.Logs = append(rf.Logs, v)
            } else {
                // overwrite
                rf.Logs[idx] = v
            }
        } else {
            // append
            rf.Logs = append(rf.Logs, v)
        }
    }

    return len(args.Entries) > 0
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) switchToCandidate() {
    // from follower to Candidate
    DPrintf("[me %d]switchToCandidate term %d, prev state %d",
            rf.me,
            rf.CurrentTerm,
            int(rf.state))
    rf.VotedFor = rf.me
    rf.CurrentTerm += 1
    rf.state = Candidate

    if rf.leaderActive {
        panic("switchToCandidate but leader active")
    } else {
        rf.leader = -1
    }

    rf.persist()
    rf.resetElectWithRandTimeout()
}

func (rf *Raft) switchToFollower() {
    DPrintf("[me %d]switchToFollower, prev state %d, leader Active %v, leaderId %d",
            rf.me,
            int(rf.state),
            rf.leaderActive,
            rf.leader)

    rf.resetElectWithRandTimeout()
    rf.state = Follower
}

func (rf *Raft) switchToLeader() {
    if rf.state == Leader {
        panic("switchToLeader but already be leader")
    }

    DPrintf("[me %d] switchToLeader term %d", rf.me, rf.CurrentTerm)
    rf.state = Leader
    rf.leader = rf.me
    if rf.electTimer != nil {
        if !rf.electTimer.Stop() && len(rf.electTimer.C) > 0 {
            <-rf.electTimer.C
        }
    }

    // For each follower, there is a routine
    // check nextIndex & log tail.
    // Because golang's cond not support timeout, we must
    // use select + channel + timer
    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    // create routine for each follower
    rf.appendNotify = make([]chan int, len(rf.peers))
    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            rf.appendNotify[i] = make(chan int, 1)
            rf.nextIndex[i] = rf.absIndex(len(rf.Logs))
            rf.matchIndex[i] = rf.absIndex(0)

            go rf.appendRoutine(i)
        }
    }
}

// usage:
// ch := checkLockWaitTooLong("my lock ")
// mutex.Lock()
// close(ch)
func checkLockWaitTooLong(s string) chan struct{} {
    // check dead lock
    ch := make(chan struct{})
    go func() {
        select {
        case <-time.After(time.Second * 3):
            DPrintf("DeadLock: " + s)
            return
            //panic("DeadLock:" + s)
        case <-ch:
            return
        }
    }()

    return ch
}

// usage:
// ch := checkLockHoldTooLong("holding my lock ")
// mutex.Unlock()
// close(ch) // or defer close(ch)
func checkLockHoldTooLong(s string) chan struct{} {
    ch := make(chan struct{})
    go func() {
        select {
        case <-time.After(time.Second * 3):
            panic("Holding Lock too long: " + s)
        case <-ch:
            return
        }
    }()

    return ch
}

// routine in leader for each follower
func (rf *Raft) appendRoutine(index int) {
    DPrintf("[me %d] ENTER appendRoutine for follower %d", rf.me, index)
    // send heart beat at once
    heartId := time.After(time.Duration(1) * time.Nanosecond)
    for {
        select {
        case <-rf.shutdown:
            return

        case <-rf.appendNotify[index]:
            // heartbeat can be reset
            heartId = time.After(rf.heartPeriod)

        case <-heartId:
            //DPrintf("[me %d] appendRoutine timeout for %d", rf.me, index)
            heartId = time.After(rf.heartPeriod)
        }

        ch := checkLockWaitTooLong("begin appendRoutine_" + strconv.Itoa(index))
        // Lock and send AppendEntries RPC to follower `index`
        rf.mu.Lock()
        close(ch)

        cState := rf.state
        // must check state here, if it isn't leader, the nextIndex may be invalid,
        // please return ASAP
        if cState != Leader {
            DPrintf("[me %d] not leader, EXIT appendRoutine_%d", rf.me, index)
            rf.mu.Unlock()
            return
        }

        cTerm := rf.CurrentTerm
        prevTerm := -1
        prevIdx := rf.nextIndex[index] - 1
        DPrintf("[me %d] appendRoutine_%d  prevIdx %d include %d", rf.me, index, prevIdx, rf.lastIncludedIndex)
        if prevIdx < rf.absIndex(0) {
            DPrintf("[me %d] use snapshot for %d, prevIdx %d, first log %d", rf.me, index, prevIdx, rf.Logs[0].Index)
            // use snapshot
            req := &InstallSnapshotArgs{Term:cTerm, LastIncludedIndex:rf.lastIncludedIndex,
                                        LeaderId:rf.me,
                                        LastIncludedTerm:rf.lastIncludedTerm, Snapshot:rf.persister.ReadSnapshot()}
            go func(index int) {
                reply := new(InstallSnapshotReply)
                ok := rf.sendInstallSnapshot(index, req, reply)

                ch := checkLockWaitTooLong("InstallSnapshot" + strconv.Itoa(index))
                rf.mu.Lock()
                close(ch)
                defer rf.mu.Unlock()
                if ok {
                    if rf.CurrentTerm != cTerm {
                        // retry
                        rf.appendNotify[index]<-0
                        return
                    }

                    if reply.Term > rf.CurrentTerm {
                        rf.CurrentTerm = reply.Term
                        rf.leader = index
                        rf.leaderActive = true
                        if rf.state != Follower {
                            rf.switchToFollower()
                        }
                        rf.persist()
                    } else {
                        // snapshot success
                        if rf.matchIndex[index] > rf.lastIncludedIndex {
                            panic("wrong matchIndex")
                        }

                        rf.nextIndex[index] = rf.lastIncludedIndex + 1
                        rf.matchIndex[index] = rf.lastIncludedIndex
                        // check
                        newCommit := rf.calcCommitIndex(index)
                        if newCommit != rf.commitIndex {
                            panic("after snapshot success, commitIndex should not change")
                        }
                    }
                } else {
                    rf.appendNotify[index]<-0 // retry
                }
            }(index)

            rf.mu.Unlock()
            return
        }

        // check
        if prevIdx >= rf.absIndex(len(rf.Logs)) {
            err := fmt.Sprintf("[me %d] follower %d, prevIdx %d, logs %v", rf.me, index, prevIdx, rf.Logs)
            panic(err)
        }

        prevTerm = rf.Logs[rf.relativeIndex(prevIdx)].Term

        // [nextIndex, len(Logs)) need to sync
        entries := rf.Logs[rf.relativeIndex(prevIdx+1):]
        leaderCommit := rf.commitIndex
        rf.mu.Unlock()

        req := &AppendEntriesArgs{Term:cTerm, LeaderId:rf.me,
                                  PrevLogIndex:prevIdx, PrevLogTerm:prevTerm,
                                  Entries:entries, LeaderCommit:leaderCommit}
        // send AppendEntries RPC
        go func(index int) {
            reply := new(AppendEntriesReply)
            if ok := rf.sendAppendEntries(index, req, reply); !ok {
                //DPrintf("[me %d] failed send appendEntries to %d, myState %d", rf.me, index, int(rf.state))
                return
            }

            ch := checkLockWaitTooLong("AppendEntries" + strconv.Itoa(index))
            rf.mu.Lock()
            close(ch)
            defer rf.mu.Unlock()

            if req.Term != rf.CurrentTerm {
                DPrintf("[me %d] AppendEntriesArgs stale term %d, now %d",
                        rf.me,
                        req.Term,
                        rf.CurrentTerm)
                return
            }

            DPrintf("[me %d] succ send appendEntries to %d with entries %v", rf.me, index, req.Entries)
            // check if stale nextIndex ?
            if rf.nextIndex[index] != req.PrevLogIndex + 1 {
                DPrintf("[me %d] recv appendEntries reply, but follow %d's nextIndex may stale now %d origin %d", rf.me, index, rf.nextIndex[index], req.PrevLogIndex + 1)
                return
            }

            if reply.Success {
                if len(req.Entries) > 0 {
                    if rf.nextIndex[index] > req.PrevLogIndex + 1 + len(req.Entries) {
                        // Example: leader recv cmd 1 2 in same time, leader issue two appendEntries:
                        // 1st is prev&term=(0,0) with entries (1)
                        // 2nd is prev&term=(0,0) with entries (1)(2)
                        // suppose 2nd's rsp return first, update nextIndex = 3
                        // when 1st's rsp return, it'll get here
                        DPrintf("[me %d] wanna update nextIndex to %d for follower %d, but now it's bigger = %d", rf.me, req.PrevLogIndex+1+len(req.Entries), index, rf.nextIndex[index])
                        return
                    }
                    // set nextIndex to prev index + len(entries)
                    rf.nextIndex[index] = req.PrevLogIndex + 1 + len(req.Entries)
                    rf.matchIndex[index] = rf.nextIndex[index] - 1
                    DPrintf("[me %d] update matchIndex to %d for follower %d, commit = %d", rf.me, rf.matchIndex[index], index, rf.commitIndex)
                    // counting quorum for update commitIndex
                    newCommit := rf.calcCommitIndex(index)
                    if newCommit != rf.commitIndex {
                        rf.commitIndex = newCommit
                        rf.applyNotify<-0
                        DPrintf("[me %d] update commitIndex to %d", rf.me, newCommit)
                    }
                }
            } else {
                if rf.CurrentTerm < reply.Term {
                    DPrintf("[me %d] bigger heartbeat reply term %d", rf.me, reply.Term)
                    rf.CurrentTerm = reply.Term
                    rf.leader = -1
                    rf.leaderActive = false

                    rf.VotedFor = -1
                    rf.persist()
                    rf.switchToFollower()
                } else {
                    //rf.nextIndex[index] -= 1
                    if rf.nextIndex[index] - reply.FirstIndexOfFailTerm > 1 {
                        DPrintf("[me %d] refused by follow %d, optimize nextIndex %d to %d", rf.me, index, rf.nextIndex[index], reply.FirstIndexOfFailTerm)
                    }
                    if rf.relativeIndex(reply.FirstIndexOfFailTerm) >= 0 {
                        rf.nextIndex[index] = reply.FirstIndexOfFailTerm
                        DPrintf("[me %d] refused by follow %d, decr next %d, leader logs %v", rf.me, index, rf.nextIndex[index], rf.Logs)
                        if rf.nextIndex[index] < rf.absIndex(len(rf.Logs)) {
                            rf.appendNotify[index]<-0
                        } else {
                            panic("ERROR: when decr nextIndex, it's not shorter than Log's length")
                        }
                    } else {
                        // use snapshot
                        DPrintf("[me %d] refused by follow %d, should use snapshot", rf.me, index)
                        rf.nextIndex[index] = -1
                        rf.appendNotify[index]<-0
                    }
                }
            }
        }(index)
    }
}

func (rf* Raft) calcCommitIndex(follower int) int {
    for ai := rf.matchIndex[follower]; ai >= rf.commitIndex+1; ai-- {
        i := rf.relativeIndex(ai)
        DPrintf("[me %d] follower %d, ai %d, i %d, rf.commit %d", rf.me, follower, ai, i, rf.commitIndex)
        if i >= len(rf.Logs) {
            err := fmt.Sprintf("[me %d] follower %d is out of len %d", rf.me, i, len(rf.Logs))
            panic(err)
        }

        if rf.Logs[i].Term != rf.CurrentTerm {
            // only count my term, see Figure 8
            continue
        }

        count := 0
        for fi := 0; fi < len(rf.matchIndex); fi++ {
            if rf.matchIndex[fi] >= ai {
                count++
            }
        }

        DPrintf("[me %d] matchIndex %d's quorum %d", rf.me, ai, count)
        if count*2 > len(rf.peers) {
            // return new commitIndex
            return ai
        }
    }

    // no new match entry, return old commitIndex
    return rf.commitIndex
}

func (rf *Raft) checkLeaderAlive() {
    for {
        select {
        case <-rf.shutdown:
            return

        case <-rf.electTimer.C:
            rf.mu.Lock()
            DPrintf("[me %d] checkLeaderAlive: onTimer leader active %v",
                    rf.me,
                    rf.leaderActive)

            if rf.state == Leader {
                // ignore this timer in leader state
                rf.mu.Unlock()
                return
            }

            if rf.leaderActive {
                rf.leaderActive = false
                rf.resetElectWithRandTimeout()
            } else {
                // To new elect, even if Candidate state
                rf.switchToCandidate()
                go rf.election()
            }

            rf.mu.Unlock()
        }
    }
}


// rf should be Candidate
func (rf *Raft) election() {
    rf.mu.Lock()
    req := &RequestVoteArgs{Term:rf.CurrentTerm, CandidateId:rf.me}
    npeers := len(rf.peers)
    req.LastLogIndex, req.LastLogTerm = rf.lastLogIndexAndTerm()
    rf.mu.Unlock()

    myVotes := 1 // no need atomic because mu.Lock

    for i := 0; i < npeers; i++ {
        select {
        case <-rf.shutdown:
            return
        default:
        }

        if i == rf.me {
            continue
        }

        go func(n int) {
            var reply RequestVoteReply
            if rf.sendRequestVote(n, req, &reply) {
                rf.mu.Lock()
                defer rf.mu.Unlock()

                if req.Term != rf.CurrentTerm {
                    DPrintf("[me %d] RequestVoteReply stale term %d, now %d",
                            rf.me,
                            req.Term,
                            rf.CurrentTerm)

                    return
                }

                DPrintf("[me %d] RequestVoteReply from %d, term %d, grant %v, votes %d",
                        rf.me,
                        n,
                        reply.Term,
                        reply.Granted,
                        myVotes)

                if rf.state != Candidate {
                    // Already enough votes, got leader or follower.
                    return
                }

                if reply.Granted {
                    myVotes += 1
                    if myVotes > len(rf.peers) / 2 {
                        rf.switchToLeader()
                    }
                } else {
                    if reply.Term > req.Term {
                        rf.CurrentTerm = reply.Term
                        rf.persist()
                        rf.leader = -1
                        rf.leaderActive = false
                        rf.switchToFollower()
                    }
                }
            } else {
                DPrintf("[me %d] failed send RequestVote to %d", rf.me, n)
            }
        }(i)
    }
}


func (rf *Raft) applyRoutine() {
    for {
        select {
        case <-rf.shutdown:
            return

        case <-rf.applyNotify:
        }

        rf.mu.Lock()
        start := rf.lastApply+1
        nApply := rf.commitIndex - rf.lastApply

        toApplyLogs := make([]LogEntry, nApply)
        relativeStart := rf.relativeIndex(start)
        copy(toApplyLogs, rf.Logs[relativeStart: relativeStart+nApply])

        rf.lastApply = rf.commitIndex
        rf.mu.Unlock()

        for i := 0; i < len(toApplyLogs); i++ {
            DPrintf("[me %d] apply log abs_index %d", rf.me, start+i)

            msg := ApplyMsg{}
            msg.CommandValid = true
            msg.Command = toApplyLogs[i].Command
            msg.CommandIndex = toApplyLogs[i].Index

            if msg.Command == nil {
                msg.CommandValid = false
            }

            rf.applyCh<-msg
        }

        /*
        //ch := checkLockHoldTooLong("applyRoutine hold lock " + strconv.Itoa(nApply))
        for i := rf.lastApply+1; i <= rf.commitIndex; i++ {
            DPrintf("[me %d] apply log abs_index %d, snapIndex %d", rf.me, i, rf.lastIncludedIndex)

            ri := rf.relativeIndex(i)
            msg := ApplyMsg{}
            msg.CommandValid = true
            msg.Command = rf.Logs[ri].Command
            msg.CommandIndex = rf.Logs[ri].Index

            if msg.Command == nil {
                msg.CommandValid = false
            }

            rf.applyCh<-msg
        }
        rf.lastApply = rf.commitIndex
        rf.mu.Unlock()
        //close(ch)
        */
    }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    // Your code here (2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()

    index := -1
    term, isLeader := rf.GetStateWithoutLock()
    if !isLeader {
        return index, term, false
    }

    // append to Logs, but commitIndex is not advanced.
    // broadcast to followers in goroutines

    index = rf.absIndex(len(rf.Logs))
    entry := LogEntry{Index:index, Term:rf.CurrentTerm, Command:command}
    rf.Logs = append(rf.Logs, entry)
    rf.persist()

    // update self
    rf.nextIndex[rf.me] = rf.absIndex(len(rf.Logs))
    rf.matchIndex[rf.me] = rf.absIndex(len(rf.Logs)) - 1

    DPrintf("[me %d] Start command idx %d", rf.me, index)

    // foreach follower, if it's nextIndex is old log tail, then advance it.
    // The dedicated append routine will loop while nextIndex != log tail
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }

        if rf.nextIndex[i] == entry.Index {
            rf.appendNotify[i]<-0 // notify append routine[i]
        }
    }

    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    // Your code here, if desired.
    close(rf.shutdown)
    DPrintf("[me %d]Kill raft", rf.me)
}

// To avoid live lock
func (rf* Raft) resetElectWithRandTimeout() {
    // 300-450ms
    electTimeout := time.Duration(300+mrand.Intn(150)) * time.Millisecond

    if rf.electTimer == nil {
        rf.electTimer = time.NewTimer(electTimeout)
        return
    }

    if !rf.electTimer.Stop() && len(rf.electTimer.C) > 0 {
        <-rf.electTimer.C
    }

    rf.electTimer.Reset(electTimeout)
}

//////////////////////////////////////
// about snapshot
func (rf* Raft) SnapshotSize() int {
    return rf.persister.SnapshotSize()
}

func (rf* Raft) RaftStateSize() int {
    return rf.persister.RaftStateSize()
}

// Snapshot RPC request
type InstallSnapshotArgs struct {
    Term int
    LeaderId int // so follower can redirect clients
    LastIncludedIndex int
    LastIncludedTerm int
    Snapshot []byte // the bytes saved in persister
}

// Snapshot RPC response
type InstallSnapshotReply struct {
    Term int
}

// convert abs log index to index ignore snapshot
func (rf* Raft) relativeIndex(absIndex int) int {
    return absIndex - rf.lastIncludedIndex
}

// convert relative log index to abs index with snapshot
func (rf* Raft) absIndex(relativeIndex int) int {
    return relativeIndex + rf.lastIncludedIndex
}

// snapshot is passed from kvserver, return bytes tobe saved in persister
func (rf *Raft) snapshotBytes(snapshot []byte, lastIndex int) []byte {
    // for snapshot
    s := new(bytes.Buffer)
    se := labgob.NewEncoder(s)

    rIndex := rf.relativeIndex(lastIndex)
    lastTerm := rf.Logs[rIndex].Term

    // save snapshot
    se.Encode(lastIndex)
    se.Encode(lastTerm)
    se.Encode(snapshot)

    return s.Bytes()
}
//
// kvserver will notify raft to save snapshot
//
func (rf *Raft) StartSnapshot(snapshot []byte, lastIndex int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    rf.startSnapshot(snapshot, lastIndex)
}

func (rf *Raft) startSnapshot(snapshot []byte, lastIndex int) {
    if lastIndex == rf.lastIncludedIndex {
        return // duplicated snapshot
    } else if lastIndex < rf.lastIncludedIndex {
        //err := fmt.Sprintf("[me %d] why snapshot index %d < lastIncludedIndex %d", rf.me, lastIndex, rf.lastIncludedIndex)
        //panic(err)
        return
    } else if lastIndex > rf.lastApply {
        panic("why snapshot index > lastApply")
    }

    rIndex := rf.relativeIndex(lastIndex)
    lastTerm := rf.Logs[rIndex].Term

    // truncate and update raft state
    // example : suppose lastIndex = 3, log is:
    // [0, 1, 2, 3, 4, 5, 6, 7], log0 is dummy placeholder.
    // now snapshot the log [0, 1, 2, 3], we should not truncate
    // the log to [4, 5, 6, 7], but [3, 4, 5, 6, 7], use log3 as placeholder,
    // and if nextIndex is 4 in absolute index( relative index is 1),
    // the prevIdx&Term should be of log3, perfect!
    //rf.Logs[rIndex].Command = nil // lastIndex entry used as dummy // data race here...
    rf.Logs = rf.Logs[rIndex:]
    rf.lastIncludedIndex = lastIndex
    rf.lastIncludedTerm = lastTerm
    // raft state
    stateBytes := rf.raftStateBytes()
    snapshotBytes := rf.snapshotBytes(snapshot, lastIndex)

    DPrintf("[me %d]save state %d, snap %d, lastIndex %d", rf.me, len(stateBytes), len(snapshotBytes), lastIndex)
    // persist
    rf.persister.SaveStateAndSnapshot(stateBytes, snapshotBytes)
}

// data is snapshot from persister
func (rf *Raft) toAppSnapshot(data []byte) []byte {
    if data == nil || len(data) < 1 {
        return []byte{}
    }
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)

    var lastIndex int
    var lastTerm int
    var snapshot []byte

    if  d.Decode(&lastIndex) != nil ||
        d.Decode(&lastTerm) != nil ||
        d.Decode(&snapshot) != nil {
        DPrintf("[me %d]: toAppSnapshot failed!", rf.me)
        panic("toAppSnapshot failed")
    } else {
        DPrintf("[me %d]: toAppSnapshot index&term (%d,%d)", rf.me, lastIndex, lastTerm)
        return snapshot
    }
}


// restore previously persisted snapshot
//
func (rf *Raft) readSnapshot(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any snapshot?
        return
    }
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)

    var lastIndex int
    var lastTerm int
    var snapshot []byte

    if  d.Decode(&lastIndex) != nil ||
        d.Decode(&lastTerm) != nil ||
        d.Decode(&snapshot) != nil {
        DPrintf("[me %d]: readSnapshot failed!", rf.me)
        panic("readSnapshot failed")
    } else {
        // mutex must be held
        rf.lastIncludedIndex = lastIndex
        rf.lastIncludedTerm = lastTerm
        rf.commitIndex = rf.lastIncludedIndex
        rf.lastApply = rf.lastIncludedIndex
        DPrintf("[me %d]: readSnapshot index %d, term %d", rf.me, lastIndex, lastTerm)

        // lab3B: Notify the kvserver
        msg := ApplyMsg{}
        msg.CommandValid = false
        msg.Snapshot = snapshot
        rf.applyCh<-msg
    }
}


// leader call this
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    return ok
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    DPrintf("[me %d]InstallSnapshot term %d, last index&term (%d,%d), my index %d, term %d, logs %v",
            rf.me,
            args.Term, args.LastIncludedIndex, args.LastIncludedTerm,
            rf.lastIncludedIndex, rf.CurrentTerm, rf.Logs)

    reply.Term = rf.CurrentTerm
    if rf.CurrentTerm > args.Term {
        rf.mu.Unlock()
        return
    }

    rf.CurrentTerm = args.Term
    rf.leader = args.LeaderId
    rf.leaderActive = true
    if rf.state != Follower {
        rf.switchToFollower()
    }

    if args.LastIncludedIndex <= rf.lastIncludedIndex {
        rf.mu.Unlock()
        return
    }

    relativeLastIndex := rf.relativeIndex(args.LastIncludedIndex)

    rf.lastIncludedIndex = args.LastIncludedIndex
    rf.lastIncludedTerm = args.LastIncludedTerm

    conflict := (relativeLastIndex < 0 || relativeLastIndex >= len(rf.Logs)) ||
                (rf.Logs[relativeLastIndex].Term != args.LastIncludedTerm)
    if conflict {
        // clear logs and use snapshot
        rf.Logs = nil
        entry := LogEntry{Index:args.LastIncludedIndex, Term:args.LastIncludedTerm, Command:nil}
        rf.Logs = append(rf.Logs, entry)

        rf.commitIndex = rf.lastIncludedIndex
        rf.lastApply = rf.lastIncludedIndex
        relativeLastIndex = 0
    } else {
        if rf.commitIndex < rf.lastIncludedIndex {
            rf.commitIndex = rf.lastIncludedIndex
            rf.lastApply = rf.lastIncludedIndex
        } else if rf.lastApply < rf.lastIncludedIndex {
            rf.lastApply = rf.lastIncludedIndex
        }
    }

    //rf.Logs[relativeLastIndex].Command = nil // lastIndex entry used as dummy
    rf.Logs = rf.Logs[relativeLastIndex:]

    rf.persister.SaveStateAndSnapshot(rf.raftStateBytes(), args.Snapshot)

    DPrintf("[me %d]After InstallSnapshot my index %d, logs %v",
            rf.me,
            rf.lastIncludedIndex, rf.Logs)

    rf.mu.Unlock()

    // lab3B: Notify the kvserver
    msg := ApplyMsg{}
    msg.CommandValid = false
    msg.Snapshot = rf.toAppSnapshot(args.Snapshot)
    rf.applyCh<-msg
}


// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {

    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me
    rf.applyCh = applyCh

    DPrintf("[me %d]: CREAT raft with num of peers %d", rf.me, len(peers))
    // Your initialization code here (2A, 2B, 2C).
    rf.CurrentTerm = 0
    rf.VotedFor = -1
    rf.leader = -1

    rf.commitIndex = 0
    rf.lastApply = 0

    rf.leaderActive = false

    // 100 ms
    rf.heartPeriod = time.Duration(100) * time.Millisecond

    rf.shutdown = make(chan interface{}, 1)
    rf.applyNotify = make(chan interface{}, 1)

    rf.switchToFollower()

    // log index is 1-based
    entry := LogEntry{Index:0, Term:0, Command:nil}
    rf.Logs = append(rf.Logs, entry)

    rf.lastIncludedIndex = rf.Logs[0].Index
    rf.lastIncludedTerm = rf.Logs[0].Term
    rf.commitIndex = rf.lastIncludedIndex
    rf.lastApply = rf.lastIncludedIndex

    // read snapshot
    // via applyCh notify kvserver to process snapshot
    rf.readSnapshot(persister.ReadSnapshot())

    // initialize from state persisted before a crash
    // CurrentTerm, VotedFor, Logs
    rf.readPersist(persister.ReadRaftState())

    // start a goroutine for check leader alive
    go rf.checkLeaderAlive()
    // start a goroutine for apply commited logs
    go rf.applyRoutine()

    return rf
}

