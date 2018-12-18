# Lab2 Raft

## Lab Part 2A

        Implement leader election and heartbeats (AppendEntries RPCs with no log entries). The goal for Part 2A is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost. Run go test -run 2A to test your 2A code.

- raft.go
1. Init state is Follower
2. Three goroutines: checkHealthy & election & heartDaemon
3. checkHealthy is for Follower state, it checks whether leader is active
4. If leader inactive, then switch to Candidate, start election goroutine
5. RequestVotes in election routine.
6. Handle RequestVotes: if Candidate, switchToFollower; if Follower, check if
had voted or same term; if Leader, check if stale leader.
7. heartbeat routine: send heartbeat periodly, if recv bigger term, switchToFollower.
