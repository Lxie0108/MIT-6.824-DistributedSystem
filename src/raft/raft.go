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
	//"fmt"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	HeartBeatInterval  = 100 * time.Millisecond
	ElectionInterval   = 150 * time.Millisecond
	ElectionTimeoutMin = 300
	ElectionTimeoutMax = 450
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

type InstallSnapshotArgs struct {
	Term              int //leader's term
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte //starts at offset
}

type InstallSnapshotReply struct {
	Term int //current term
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
	snapshotIndex int
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
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)

	data := w.Bytes()
	return data
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
	var snapshotIndex int
	if d.Decode(&currentTerm) != nil || d. Decode(&votedFor) != nil ||
	   d.Decode(&log) != nil || d.Decode(&snapshotIndex) != nil{
	   //error...
	   DPrintf("%v fails to read persist states", rf)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotIndex = snapshotIndex
	}
}

//Raft leaders must sometimes tell lagging Raft peers to update their state by installing a snapshot. 
//You need to implement InstallSnapshot RPC senders and handlers for installing snapshots when this situation arises. 
//This is in contrast to AppendEntries, which sends log entries that are then applied one by one by the service
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex <rf.snapshotIndex{ //Reply immediately if term < currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo("Follower")
	}

	//If existing log entry has same index and term as snapshot’slast included entry, retain log entries following it and reply
	if len(rf.log) > args.LastIncludedIndex - rf.snapshotIndex && rf.log[args.LastIncludedIndex - rf.snapshotIndex].Term == args.LastIncludedTerm {
		rf.log = rf.log[args.LastIncludedIndex - rf.snapshotIndex:]
	} else { //Discard the entire log
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}
	//Save snapshot file, discard any existing or partial snapshot with a smaller index
	rf.snapshotIndex = args.LastIncludedIndex	
	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.snapshotIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)
	if rf.lastApplied > rf.snapshotIndex {
		return
	}

	//The InstallSnapshot handler can use the applyCh to send the snapshot to the service, by putting the snapshot in ApplyMsg.
	//The service reads from applyCh, and invokes CondInstallSnapshot with the snapshot to tell Raft that the service is switching to the passed-in snapshot state, and that Raft should update its log at the same time
	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid: false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	/**if !rf.isNewer(lastIncludedTerm, lastIncludedIndex) { // older snapshots must be refused
		return false
	} //else it is recent
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.log = []LogEntry{}
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)**/
	return true
}

//helper method. given lastLogTerm and lastLogIndex it decides if it is a newer log
func (rf *Raft) isNewer(lastLogTerm, lastLogIndex int) bool {
	/**term := rf.lastIncludedTerm
	index := rf.lastIncludedIndex
	if lastLogTerm > term {
		return true
	}
	if lastLogTerm == term {
		return lastLogIndex >= index
	}**/
	return false
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotIndex {
		return
	}
	rf.log = rf.log[index - rf.snapshotIndex:]
	rf.snapshotIndex = index
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.installSnapshotToServer(i)
	}
}

func (rf *Raft) installSnapshotToServer(server int) {
	rf.mu.Lock()
	if rf.state != "Leader" {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	if rf.sendInstallSnapshot(server, &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.convertTo("Follower")
			rf.persist()
		} else {
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
		rf.mu.Unlock()
	}
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
		rf.convertTo("Follower")
		rf.persist()
	}

	rf.electionTimer.Reset(rf.getRandomDuration())

	if args.PrevLogIndex <= rf.snapshotIndex {
		reply.Success = true
		if args.PrevLogIndex +len(args.Entries) > rf.snapshotIndex {
			startIndex := rf.snapshotIndex - args.PrevLogIndex
			rf.log = rf.log[:1]
			rf.log = append(rf.log, args.Entries[startIndex:]...)
		}
		return
	}

	//2B 
	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		if len(rf.log) - 1 + rf.snapshotIndex < args.PrevLogIndex { //If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictIndex = len(rf.log) + rf.snapshotIndex
			reply.ConflictTerm = -1	
			return		
		} 
		if rf.log[args.PrevLogIndex - rf.snapshotIndex].Term != args.PrevLogTerm {
		//If a follower does have prevLogIndex in its log, but the term does not match, it should return conflictTerm = log[prevLogIndex].Term, 
		//and then search its log for the first index whose entry has term equal to conflictTerm.
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictTerm = rf.log[args.PrevLogIndex - rf.snapshotIndex].Term // it unmatches leader's term
			xIndex := args.PrevLogIndex
		
			for rf.log[xIndex-1-rf.snapshotIndex].Term == reply.ConflictTerm {
				xIndex--
				if xIndex == rf.snapshotIndex+1 {
					break
				}
			}
			reply.ConflictIndex = xIndex
			return
		}

	

	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	conflict := false
	conflictIdx := -1
	for i := range args.Entries {
		if len(rf.log) < args.PrevLogIndex+2+i-rf.snapshotIndex || rf.log[args.PrevLogIndex+1+i-rf.snapshotIndex].Term != args.Entries[i].Term {
			//rf.log = rf.log[:curr]
			conflict = true
			conflictIdx = i
			break
		}
	}

	//deal with conflict logs
	if conflict {
		rf.log = rf.log[:args.PrevLogIndex+1+conflictIdx-rf.snapshotIndex] //delete the conflict entries
		rf.log = append(rf.log, args.Entries[conflictIdx:]...) //append with leader's
		rf.persist()
	}

	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= len(rf.log)-1 + rf.snapshotIndex{
			rf.applyCommitted(args.LeaderCommit)
		} else {
			rf.applyCommitted(len(rf.log) - 1 + rf.snapshotIndex)
			
		}
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
	if (rf.votedFor != -1 || rf.votedFor != args.CandidateId) && (args.LastLogTerm < rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1 + rf.snapshotIndex)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
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
				rf.nextIndex[i] = len(rf.log) + rf.snapshotIndex
			}
			for i := range rf.matchIndex {
				rf.matchIndex[i] = rf.snapshotIndex
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

//similar to sender above
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		isLeader = true
		index = len(rf.log) + rf.snapshotIndex
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
		rf.mu.Unlock()
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
		LastLogIndex: len(rf.log) - 1 + rf.snapshotIndex,
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
			
			if prev < rf.snapshotIndex {
				rf.mu.Unlock()
				rf.installSnapshotToServer(server)
				return
			}
			entries := make([]LogEntry, len(rf.log[prev+1-rf.snapshotIndex:]))
			copy(entries, rf.log[prev+1 - rf.snapshotIndex:])
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prev,
				PrevLogTerm:  rf.log[prev-rf.snapshotIndex].Term,
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

					for N := len(rf.log) - 1 + rf.snapshotIndex; N > rf.commitIndex; N-- {
						count := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= N {
								count++
							}
						}
						if count > len(rf.peers)/2 {
							if rf.log[N-rf.snapshotIndex].Term == rf.currentTerm{
								rf.applyCommitted(N)
								break
							}
						}
					}
				} else { //unsuccessful reply can be caused by 1. rf.currentTerm < reply.Term (2A)
					//and 2. If AppendEntries fails because of log inconsistency (2B), in this case, decrement nextIndex and retry
					if rf.currentTerm < reply.Term { //revert to Follower
						rf.currentTerm = reply.Term
						rf.convertTo("Follower")
						rf.persist()
					} else {
						//Upon receiving a conflict response, the leader should first search its log for conflictTerm. 
						//If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
						//If it does not find an entry with that term, it should set nextIndex = conflictIndex.
						rf.nextIndex[server] = reply.ConflictIndex
						if reply.ConflictTerm != -1 {
							for i := args.PrevLogIndex; i >= rf.snapshotIndex+1; i-- {
								if rf.log[i-1-rf.snapshotIndex].Term == reply.ConflictTerm {
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
func (rf *Raft) applyCommitted(newCommitIndex int) {
	//If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	rf.commitIndex = newCommitIndex
	if rf.commitIndex > rf.lastApplied {
		startIndex := rf.lastApplied + 1 - rf.snapshotIndex
		endIndex := rf.commitIndex + 1 - rf.snapshotIndex
		entries := append([]LogEntry{}, rf.log[startIndex:endIndex]...)
		go func(start int, Logentry []LogEntry) {
			for i, entry := range Logentry {
				var msg ApplyMsg
				if entry.Command == nil {
					msg.CommandValid = false
				} else {
					msg.CommandValid = true
				}
				msg.Command = entry.Command
				msg.CommandIndex = start + i
				rf.applyCh <- msg
				rf.mu.Lock()
				rf.lastApplied = msg.CommandIndex
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, entries)
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

