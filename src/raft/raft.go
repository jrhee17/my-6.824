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
	"log"
	"sort"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	currentTerm int
	votedFor    int
	log         []Log

	// volatile all
	commitIndex int // the index of the highest log entry commited
	lastApplied int // index of the highest log entry applied to the state machine

	// volatile for leaders
	nextIndex  []int
	matchIndex []int

	// custom
	state            State
	lastStateUpdated time.Time
	electionTimer    *time.Timer
	leaderTimer      *time.Timer

	applyCh chan ApplyMsg
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type State int

const (
	NONE      State = 0
	FOLLOWER  State = 1
	CANDIDATE State = 2
	LEADER    State = 3
)

type Log struct {
	Data interface{}
	Term int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Reason  string
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int

	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	Reason      string
	// Your data here (2A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[SERVER (%v)] (%v) %v -> %v: RequestVote(%+v)", rf.me, rf.currentTerm, args.CandidateId, rf.me, args)

	if args.Term < rf.currentTerm {
		reply.VoteGranted, reply.Term, reply.Reason = false, rf.currentTerm, "smaller term"
		return
	}
	lastLog := rf.log[len(rf.log)-1]
	if args.LastLogTerm < lastLog.Term {
		reply.VoteGranted, reply.Term, reply.Reason = false, rf.currentTerm, "smaller log term"
		return
	} else if args.LastLogTerm == lastLog.Term && args.LastLogIndex < len(rf.log)-1 {
		reply.VoteGranted, reply.Term, reply.Reason = false, rf.currentTerm, "smaller log index"
		return
	}
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted, reply.Term, reply.Reason = false, rf.currentTerm, "already voted"
		return
	}
	if args.Term > rf.currentTerm {
		rf.updateState(FOLLOWER, rf.currentTerm, args.Term, "server request vote")
	}
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	rf.votedFor = args.CandidateId
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[SERVER (%v)] (%v) %v -> %v: AppendEntries(%+v)", rf.me, rf.currentTerm, args.LeaderId, rf.me, args)

	if rf.currentTerm > args.Term {
		reply.Term, reply.Success, reply.Index, reply.Reason = rf.currentTerm, false, -1, "smaller term"
		return
	}
	rf.updateState(FOLLOWER, rf.currentTerm, args.Term, "server append entry")
	rf.votedFor = args.LeaderId
	if args.PrevLogIndex >= len(rf.log) {
		reply.Term, reply.Success, reply.Index, reply.Reason = rf.currentTerm, false, rf.commitIndex+1, "try again with smaller log index"
		return
	}
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Term, reply.Success, reply.Index, reply.Reason = rf.currentTerm, false, rf.commitIndex+1, "term differs for index"
		return
	}

	if len(args.Entries) > 0 {
		if args.PrevLogIndex < rf.commitIndex {
			panic("trying to rewrite committed data")
		}
		for i, entry := range args.Entries {
			idx := i + args.PrevLogIndex + 1
			if idx >= len(rf.log) {
				rf.log = append(rf.log, entry)
			} else {
				rf.log[idx] = entry
			}
		}
	}
	if rf.commitIndex != args.LeaderCommit {
		rf.commitIndex = max(rf.commitIndex, args.LeaderCommit)
		rf.applyCommits()
		log.Printf("[COMMITS (%v)] (%v) commitIndex: %v, lastApplied: %v", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	}

	*reply = AppendEntriesReply{
		Term:    args.Term,
		Success: true,
		Index:   -1,
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.rpcCall(server, "Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.rpcCall(server, "Raft.AppendEntries", args, reply)
}

func (rf *Raft) rpcCall(server int, svcMeth string, args interface{}, reply interface{}) bool {
	rf.mu.Lock()
	log.Printf("[CLIENT (%v) [%v]] (%v) [SEND] (%v -> %v)", rf.me, rf.currentTerm, svcMeth, rf.me, server)
	rf.mu.Unlock()
	ok := rf.peers[server].Call(svcMeth, args, reply)
	rf.mu.Lock()
	log.Printf("[CLIENT (%v) [%v]] (%v) [RECEIVE] (%v <- %v): (%v) %+v", rf.me, rf.currentTerm, svcMeth, rf.me, server, ok, reply)
	rf.mu.Unlock()
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, rf.currentTerm, false
	}

	index := rf.nextIndex[rf.me]
	rf.log = append(rf.log, Log{
		Data: command,
		Term: rf.currentTerm,
	})
	rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me] = index

	log.Printf("[LEADER (%v) [%v]] Start command for index: (%v) (%v)", rf.me, rf.currentTerm, index, rf.log)
	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) onStateUpdate(from State, to State) {
	if from != LEADER && to == LEADER {
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for server, _ := range rf.peers {
			rf.nextIndex[server] = len(rf.log)
			rf.matchIndex[server] = 0
		}
	}
}

func (rf *Raft) updateState(state State, fromTerm int, toTerm int, reason string) bool {
	if state == NONE {
		panic("attempted to update to NONE state")
	}
	if rf.currentTerm == fromTerm {
		log.Printf("[UPDATE_STATE (%v)] State: (%+v -> %+v), Term: (%v -> %v), Reason: %v", rf.me, rf.state, state, fromTerm, toTerm, reason)
		rf.onStateUpdate(rf.state, state)
		rf.state = state
		rf.currentTerm = toTerm
		rf.lastStateUpdated = time.Now()
		return true
	}
	return false
}

// assumes that a lock is already held
func (rf *Raft) performElection() int {
	term := rf.currentTerm
	replyTerm := rf.currentTerm
	rf.electionTimer = time.NewTimer((time.Duration(50) * time.Millisecond))
	expired := false

	cond := sync.NewCond(&rf.mu)
	votes := 1
	processed := 1
	go func() {
		<-rf.electionTimer.C
		rf.mu.Lock()
		defer rf.mu.Unlock()
		expired = true
		cond.Broadcast()
	}()
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ret := rf.sendRequestVote(server, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if ret && reply.VoteGranted {
				votes++
			}
			if reply.Term > rf.currentTerm {
				replyTerm = reply.Term
			}
			processed++
			cond.Broadcast()
		}(server)
	}

	for processed != len(rf.peers) && votes <= len(rf.peers)/2 && rf.currentTerm == term &&
		rf.currentTerm >= replyTerm && !expired {
		cond.Wait()
	}
	if replyTerm > rf.currentTerm {
		rf.updateState(FOLLOWER, rf.currentTerm, replyTerm, "client request vote")
	}
	return votes
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		rf.mu.Lock()

		prevTerm := rf.currentTerm
		if rf.state == CANDIDATE {
			DPrintf("[CANDIDATE (%v)]", rf.me)
			rf.votedFor = rf.me
			votes := rf.performElection()
			log.Printf("[VOTES (%v)] votes: %v", rf.me, votes)
			if rf.state == CANDIDATE && votes > len(rf.peers)/2 {
				rf.updateState(LEADER, prevTerm, prevTerm+1, "election won")
			} else if rf.state == CANDIDATE {
				rf.updateState(CANDIDATE, prevTerm, prevTerm+1, "election lost")
			}
		} else if rf.state == FOLLOWER {
			DPrintf("[FOLLOWER (%v)]", rf.me)
			if rf.votedFor == -1 && time.Now().After(rf.lastStateUpdated.Add(time.Duration(100)*time.Millisecond)) {
				rf.updateState(CANDIDATE, prevTerm, prevTerm+1, "election timeout")
			}
		}
		rf.votedFor = -1

		rf.mu.Unlock()

		ms := 110 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) leaderTicker() {
	for rf.killed() == false {

		rf.mu.Lock()

		if rf.state == LEADER {
			DPrintf("[LEADER (%v)]", rf.me)
			term := rf.currentTerm
			replyTerm := rf.currentTerm
			cond := sync.NewCond(&rf.mu)

			rf.leaderTimer = time.NewTimer((time.Duration(50) * time.Millisecond))
			expired := false
			go func() {
				<-rf.leaderTimer.C
				rf.mu.Lock()
				defer rf.mu.Unlock()
				expired = true
				cond.Broadcast()
			}()

			processed := 1
			success := 1
			for server, _ := range rf.peers {
				if server == rf.me {
					continue
				}
				toIdx := len(rf.log) - 1
				lastIdx := rf.nextIndex[server] - 1
				lastLog := rf.log[lastIdx]
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: lastIdx,
					PrevLogTerm:  lastLog.Term,
					Entries:      rf.log[rf.nextIndex[server]:],
					LeaderCommit: rf.commitIndex,
				}
				go func(server int) {
					reply := AppendEntriesReply{}
					ret := rf.sendAppendEntries(server, &args, &reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.state == LEADER && ret && reply.Term > rf.currentTerm {
						replyTerm = max(replyTerm, reply.Term)
					}
					if ret && reply.Success {
						success++
						rf.matchIndex[server] = max(rf.matchIndex[server], toIdx)
						rf.nextIndex[server] = max(rf.nextIndex[server], toIdx+1)
					}
					if ret && rf.currentTerm == term && !reply.Success {
						if reply.Index >= 0 {
							rf.nextIndex[server] = max(min(rf.nextIndex[server]-1, reply.Index), 1)
						} else {
							rf.nextIndex[server] = max(rf.nextIndex[server]-1, 1)
						}
					}
					processed++
					cond.Broadcast()
				}(server)
			}

			for processed != len(rf.peers) && success <= len(rf.peers)/2 && term == rf.currentTerm && rf.currentTerm >= replyTerm && !expired {
				cond.Wait()
			}
			if replyTerm > rf.currentTerm {
				rf.updateState(FOLLOWER, rf.currentTerm, replyTerm, "append client term exceeded")
			}
		}

		rf.updateIndices()

		rf.mu.Unlock()
		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) updateIndices() {
	if rf.state != LEADER {
		return
	}
	sz := len(rf.matchIndex)
	if sz == 0 {
		return
	}
	tmp := make([]int, sz)
	copy(tmp, rf.matchIndex)
	sort.Ints(tmp)
	newCommitIndex := tmp[sz/2]
	log.Printf("[INDICES (%v)] commit: (%v -> %v) %+v", rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex)

	// ensure that commit index isn't going backwards
	if rf.commitIndex >= newCommitIndex {
		// no updates
		return
	}
	rf.commitIndex = newCommitIndex
	rf.applyCommits()
}

func (rf *Raft) applyCommits() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[i].Data,
			CommandIndex:  i,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.log = []Log{{nil, -1}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.leaderTicker()

	return rf
}
