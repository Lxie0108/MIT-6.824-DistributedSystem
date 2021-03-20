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

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	HeartBeatInterval  = 100 * time.Millisecond
	ElectionInterval   = 150 * time.Millisecond
	ElectionTimeoutMin = 400
	ElectionTimeoutMax = 550
)

// import "bytes"
// import "6.824/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm    int
	votedFor       int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	state          string
	log            []LogEntry
	commitIndex    int
	lastApplied    int
	nextIndex      []int
	matchIndex     []int
	applyCh        chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == "Leader" {
		isleader = true
	} else {
		isleader = false
	}
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d. Decode(&votedFor) != nil ||
	   d.Decode(&log) != nil {
	   //error...
	   DPrintf("%v fails to read persist states", rf)
	} else {
	   rf.currentTerm = currentTerm
	   rf.votedFor = votedFor
	   rf.log = log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //candidate received vote or not
}

// AppendEntries RPC argument structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int
	Success bool
	ConflictIndex int
	ConflictTerm int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//AppendEntries RPC, a new gorountie, protect with mutex
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { //Reply false if term < currentTerm (§5.1)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.persist()
		rf.convertTo("Follower")
	}

	rf.electionTimer.Reset(rf.getRandomDuration())

	//2B
	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		if len(rf.log) <= args.PrevLogIndex {
			//no conflict
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1			
		} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term // it unmatches leader's term
			for i := args.PrevLogIndex; i > 0; i-- { //starts at PrevLogIndex and searches back
				if rf.log[i].Term == rf.log[args.PrevLogIndex].Term {
					reply.ConflictIndex = i
				} else {
					break
				}
			}
		}
		return
	}
	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	for i := range args.Entries {
		curr := args.PrevLogIndex + 1 + i
		if curr >= len(rf.log) { // index out of range
			break
		}
		if rf.log[curr].Term != args.Entries[i].Term {
			rf.log = rf.log[:curr]
			break
		}
	}

	//Append any new entries not already in the log
	for i := range args.Entries {
		rf.log = append(rf.log, args.Entries[i])
	}
	rf.persist()

	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
		rf.applyCommitted()
	}
	reply.Success = true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo("Follower")
		rf.persist()
	}
	//If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor != -1 || rf.votedFor != args.CandidateId) && (args.LastLogTerm < rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.persist()
	rf.electionTimer.Reset(rf.getRandomDuration())
}

func (rf *Raft) getRandomDuration() time.Duration {
	return time.Duration(rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)+ElectionTimeoutMin) * time.Millisecond
}

//convert the state of the server: "Follower", "Candidate", "Leader"
func (rf *Raft) convertTo(state string) {
	if state == rf.state {
		return
	} else {
		switch state {
		case "Follower":
			rf.heartbeatTimer.Stop()
			rf.electionTimer.Reset(rf.getRandomDuration())
			rf.votedFor = -1 // reset
		case "Candidate": //On conversion to candidate, start election
			rf.doElection()
		case "Leader":
			//initialize nextIndex and matchIndex
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
			}
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
			}
			rf.electionTimer.Stop()
			rf.broadcastHeartbeat()
			rf.heartbeatTimer.Reset(HeartBeatInterval)
		default:
			return
		}
		rf.state = state
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

//similar to sendRequestVote
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
	isLeader := false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).
	term = rf.currentTerm
	if rf.state == "Leader" {
		isLeader = true
		index = len(rf.log)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//start periodic election
/**On conversion to candidate, start election:
• Increment currentTerm
• Vote for self
• Reset election timer
• Send RequestVote RPCs to all other servers
• If votes received from majority of servers: become leader
• If AppendEntries RPC received from new leader: convert to
follower
• If election timeout elapses: start new election**/
func (rf *Raft) doElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	nVotes := 1
	rf.electionTimer.Reset(rf.getRandomDuration())
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	nMajority := len(rf.peers) / 2
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int, nVotes *int) {

			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted && rf.state == "Candidate" {
					*nVotes += 1
					if *nVotes > nMajority {
						rf.convertTo("Leader")
					}
				} else {
					if rf.currentTerm < reply.Term { //revert to Follower
						rf.currentTerm = args.Term
						rf.convertTo("Follower")
						rf.persist()
					}
				}
			}
		}(i, &nVotes)
	}
}

/**
send initial empty AppendEntries RPCs
(heartbeat) to each server; repeat during idle periods to
prevent election timeouts (§5.2)**/
func (rf *Raft) broadcastHeartbeat() {
	if rf.state != "Leader" {
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			prev := rf.nextIndex[server] - 1 // PreviousLogIndex should be index of log entry immediately preceding new ones.
			//It should be the last index that Follower has been update-to-date with the leader, therefore, it is nextIndex[Follower] - 1.
			var idx int
			if rf.nextIndex[server] < 0 {
				idx = 0
			} else {
				idx = rf.nextIndex[server]
			}
			entries := rf.log[idx:]
			if prev < 0 {
				prev = 0
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prev,
				PrevLogTerm:  rf.log[prev].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Success {
					//update nextIndex and matchIndex for follower
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

					//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
					for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
						count := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= N {
								count++
							}
						}
						if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
							rf.commitIndex = N
							rf.applyCommitted()
						}
					}

				} else { //unsuccessful reply can be caused by 1. rf.currentTerm < reply.Term (2A)
					//and 2. If AppendEntries fails because of log inconsistency (2B), in this case, decrement nextIndex and retry
					if rf.currentTerm < reply.Term { //revert to Follower
						rf.currentTerm = reply.Term
						rf.convertTo("Follower")
						rf.persist()
					} else {
						//rf.nextIndex[server]--
						rf.nextIndex[server] = reply.ConflictIndex
						if reply.ConflictTerm != -1 {
							for i := args.PrevLogIndex; i > 0; i-- {
								if rf.log[i-1].Term == reply.ConflictTerm {
									rf.nextIndex[server] = i
									break
								}
							}
						}
					}
				}
			}
		}(i)
	}
}

//Apply the commited LogEntry. Use applyCh to send ApplyMsg.
func (rf *Raft) applyCommitted() {
	//If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	if rf.commitIndex > rf.lastApplied {
		go func(start int, Logentry []LogEntry) {
			for i, entry := range Logentry {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: i + start,
				}
				rf.applyCh <- msg
				rf.mu.Lock()
				rf.lastApplied = msg.CommandIndex
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, rf.log[rf.lastApplied+1:rf.commitIndex+1])
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state == "Candidate" {
				rf.doElection() //If election timeout elapses, candidate starts election
			}
			if rf.state == "Follower" { //If election timeout elapses, follower converts to candidate
				rf.convertTo("Candidate")
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			rf.broadcastHeartbeat()
			rf.heartbeatTimer.Reset(HeartBeatInterval)
			rf.mu.Unlock()
		}
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(rf.getRandomDuration())
	rf.heartbeatTimer = time.NewTimer(HeartBeatInterval)
	rf.state = "Follower"
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
