# Lab2 Raft

## Lab Part 2A

        Implement leader election and heartbeats (AppendEntries RPCs with no log entries). The goal for Part 2A is for a single leader to be elected, for
the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader
are lost. Run go test -run 2A to test your 2A code.

- raft.go
1. Init state is Follower
2. Three goroutines: checkHealthy & election & heartDaemon
3. checkHealthy is for Follower state, it checks whether leader is active
4. If leader inactive, then switch to Candidate, start election goroutine
5. RequestVotes in election routine.
6. Handle RequestVotes: if Candidate, switchToFollower; if Follower, check if
had voted or same term; if Leader, check if stale leader.
7. heartbeat routine: send heartbeat periodly, if recv bigger term, switchToFollower.

## Lab Part 2B

        Implement the leader and follower code to append new log entries. This will involve implementing Start(), completing the AppendEntries RPC
structs, sending them, fleshing out the AppendEntry RPC handler, and advancing the commitIndex at the leader. Your first goal should be to pass the
TestBasicAgree() test (in test_test.go). Once you have that working, you should get all the 2B tests to pass (go test -run 2B).

- raft.go
1. If recv AppendEntries with multiple new Entries, say 3 entries, and PrevLogIndex is 10, what if rf.log[PrevLogIndex + 0].Term == Entries[0].Term,
    But rf.log[PrevLogIndex + 1].Term != Entries[1].Term, is it possible?
2. A call to Start() at the leader starts the process of adding a new operation to the log; the leader sends the new operation to the other servers in AppendEntries RPCs.
   sends on the applyCh in a goroutine.
3. Pay attention that log index is 1-based in thesis.

目前2C的unreliable figure 8测试，约有1%的几率超时
通过比对正常日志和超时日志，正常日志中，投票被拒绝的记录约67个，异常日志中，该记录有160个：
[me 0] RequestVoteReply from 2, term 123, grant false, votes 2
上面是异常的最后一条记录，term达到了123；而正常记录中，最大term只有80；
基本可以断定是没有pre-vote机制导致的，有空加上测试。目前平均100-200次失败一次
理论上pre vote之后，失败率可以降为0
但有个疑点是，并没有"bigger heartbeat reply term"，也就是leader发送心跳，却收到失败回复，因为有更大的term在follower中。
