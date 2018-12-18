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

import "sync"
import "labrpc"

// import "bytes"
// import "labgob"



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
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// log entry
type LogEntry struct {
    index int
    term int
    command interface{}
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
    currentTerm int
    votedFor int
    logs []LogEntry // log entry array

    // volatile on all servers 
    commitIndex int
    lastApply int
    state RaftState
    leaderActive bool
    electTimeout time.Duration
    electTimer *time.Timer

    heartPeriod time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    term = rf.currentTerm
    isleader = (rf.state == Leader)
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term int
    CandidateId int
    // TODO log term & index
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
    DPrintf("[me %d]RequestVote handle term %d, candi %d, my term %d, state %d",
    rf.me,
    args.Term, args.CandidateId,
    rf.currentTerm, int(rf.state))
	// Your code here (2A, 2B).
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Granted = false
    } else {
        switch rf.state {
        case Candidate:
            rf.currentTerm = args.Term
            rf.switchToFollower()
        case Follower:
            DPrintf("Follower %d recv RequestVote for %d, term %d",
                    rf.me,
                    rf.votedFor,
                    rf.currentTerm)
            if rf.currentTerm < args.Term {
                rf.currentTerm = args.Term
                rf.votedFor = args.CandidateId
                reply.Term = args.Term
                reply.Granted = true

                // new term, reset electTimer
                DPrintf("Follower %d reset electTimer voteFor %d, now term %d",
                        rf.me,
                        rf.votedFor,
                        rf.currentTerm)

                if !rf.electTimer.Stop() && len(rf.electTimer.C) > 0 {
                    <-rf.electTimer.C
                }

                rf.electTimer.Reset(rf.electTimeout)
            } else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
                // same term
                rf.currentTerm = args.Term
                rf.votedFor = args.CandidateId
                reply.Term = args.Term
                reply.Granted = true
            } else {
                // same term can only voted for one
                reply.Term = rf.currentTerm
                reply.Granted = false
            }

        case Leader:
            if args.Term > rf.currentTerm {
                DPrintf("[me %d] recv RequestVote big term %d, I am stale leader", rf.me, args.Term)
                reply.Term = args.Term
                reply.Granted = true
                rf.votedFor = args.CandidateId
                rf.currentTerm = args.Term
                rf.switchToFollower()
            } else {
                DPrintf("[me %d] I am leader, recv RequestVote same term %d, refuse it", rf.me, args.Term)
                reply.Term = rf.currentTerm
                reply.Granted = false
            }
        }
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
    // prevLogIndex
    // entries[]
    // leaderCommit
}
type AppendEntriesReply struct {
     Term int // currentTerm, for leader to update itself
     Success bool // true if follower contained entry matching prevLogIndex/Term
}
//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    DPrintf("[me %d] AppendEntries args term %d, leader Id %d, my term %d, state %d",
    rf.me,
    args.Term, args.LeaderId,
    rf.currentTerm, int(rf.state))

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }

    // TODO check log conflict

    reply.Term = args.Term
    reply.Success = true

    switch rf.state {
        case Candidate:
            rf.leaderActive = true
            rf.currentTerm = args.Term
            rf.switchToFollower()

        case Follower:
            // TODO : panic if not same leader?
            rf.currentTerm = args.Term
            rf.leaderActive = true

        case Leader:
            if rf.currentTerm < args.Term {
                rf.currentTerm = args.Term
                rf.switchToFollower()
            } else {
                reply.Success = false
                panic("Another leader in same term")
            }
    }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) switchToCandidate() {
    // from follower to Candidate

    DPrintf("[me %d]switchToCandidate term %d, prev state %d",
            rf.me,
            rf.currentTerm,
            int(rf.state))
    rf.votedFor = -1
    rf.currentTerm += 1
    rf.state = Candidate

    if rf.leaderActive {
        panic("switchToCandidate but leader active")
    }

    if !rf.electTimer.Stop() && len(rf.electTimer.C) > 0 {
        <-rf.electTimer.C
    }
    DPrintf("[me %d] switchToCandidate and reset elect timer", rf.me)
    rf.electTimer.Reset(rf.electTimeout)
}

func (rf *Raft) switchToFollower() {
    if rf.electTimer == nil {
        DPrintf("[me %d]switchToFollower nil electTimer", rf.me)
        rf.electTimer = time.NewTimer(rf.electTimeout)
    } else {
        DPrintf("[me %d]switchToFollower with electTimer, prev state %d, leader Active %v",
                rf.me,
                int(rf.state),
                rf.leaderActive)
        if !rf.electTimer.Stop() && len(rf.electTimer.C) > 0 {
            <-rf.electTimer.C
        }

        rf.electTimer.Reset(rf.electTimeout)
    }

    rf.state = Follower
}

func (rf *Raft) switchToLeader() {
    DPrintf("[me %d] switchToLeader term %d", rf.me, rf.currentTerm)
    rf.state = Leader
    if rf.electTimer != nil {
        if !rf.electTimer.Stop() && len(rf.electTimer.C) > 0 {
            <-rf.electTimer.C
        }
    }

    go rf.heartDaemon()
}

func (rf *Raft) checkHealthy() {
    for {
        if rf.state == Leader {
            if !rf.electTimer.Stop() && len(rf.electTimer.C) > 0 {
                // already expired so retrieved channel
                <-rf.electTimer.C
            }

            return
        }

        select {
        case <-rf.electTimer.C:
            DPrintf("[me %d] checkHealthy: onTimer leader active %v",
                    rf.me,
                    rf.leaderActive)
            if rf.leaderActive {
                rf.leaderActive = false

                if !rf.electTimer.Stop() && len(rf.electTimer.C) > 0 {
                    <-rf.electTimer.C
                }
                rf.electTimer.Reset(rf.electTimeout)
            } else {
                // To new elect
                rf.switchToCandidate()
                go rf.election()
            }
        }
    }
}


func (rf *Raft) election() {
    req := &RequestVoteArgs{Term:rf.currentTerm, CandidateId:rf.me}
    rf.votedFor = rf.me
    myVotes := 1 // atomic int

    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }

        go func(n int) {
            if rf.state == Leader {
                return
            }

            var reply RequestVoteReply
            if rf.sendRequestVote(n, req, &reply) {
                if rf.state != Candidate {
                    return
                }

                DPrintf("[me %d] RequestVoteReply term %d, grant %v, votes %d",
                        rf.me,
                        reply.Term,
                        reply.Granted,
                        myVotes)
                if rf.state == Leader {
                    return
                }

                if reply.Granted {
                    myVotes += 1
                    if myVotes >= len(rf.peers) / 2 {
                        rf.switchToLeader()
                    }
                } else {
                    if reply.Term > req.Term {
                        rf.currentTerm = reply.Term
                        rf.switchToFollower()
                    }
                }
            }
        }(i)
    }
}

// only for leader
func (rf *Raft) heartDaemon() {
    DPrintf("[me %d] ENTER heartDaemon state %d", rf.me, int(rf.state))
    for {
        if rf.state != Leader {
            DPrintf("[me %d] not leader, EXIT heartDaemon", rf.me)
            return
        }

        req := &AppendEntriesArgs{Term : rf.currentTerm, LeaderId : rf.me}
        for i := 0; i < len(rf.peers) && rf.state == Leader; i++ {
            if i == rf.me {
                continue
            }

            //!!! because sendAppendEntries may be blocked
            go func(i int) {
                // send heartbeat
                reply := new(AppendEntriesReply)
                if ok := rf.sendAppendEntries(i, req, reply); !ok {
                    DPrintf("[me %d] failed send heartbeat to %d", rf.me, i)
                } else {
                    DPrintf("[me %d] succ send heartbeat to %d", rf.me, i)
                    if !reply.Success {
                        DPrintf("[me %d] bigger heartbeat reply term %d", rf.me, reply.Term)
                        rf.currentTerm = reply.Term
                        rf.switchToFollower()
                    }
                }
            }(i)
        }

        time.Sleep(rf.heartPeriod)
    }
}
//

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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
}

//
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

    mrand.Seed(time.Now().UnixNano())

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
    rf.currentTerm = 0 // for 2A, not persist
    rf.votedFor = -1

    // 300-500ms
    rf.leaderActive = false
    rf.electTimeout = time.Duration(300+mrand.Intn(200)) * time.Millisecond
    // 100 ms
    rf.heartPeriod = time.Duration(100) * time.Millisecond

    rf.switchToFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    // start a goroutine for check leader alive
    go rf.checkHealthy()

	return rf
}

