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
	"bytes"
	// "fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	
	// self-defined variables
	isLeader bool
	isCandidate bool
	timeOut   bool               // time out for election
	voteCount int                // number of votes received
	applyCh   chan ApplyMsg      // applyCh for tester to receive messages
	

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log 	   []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int


	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int




	

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.currentTerm, rf.isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var raftlog []LogEntry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&raftlog) != nil{
		log.Fatal("Error in decoding")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.log = raftlog
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int 
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) updateStateMachine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.lastApplied {
		DPrintf("Server %d  is updating state machine from  %d  to %d", rf.me, rf.lastApplied, rf.commitIndex)
		DPrintf("%v", rf.log)
		DPrintf("%v", rf.isLeader)

		tmp_log := make([]LogEntry, len(rf.log))
		copy(tmp_log, rf.log)
		rf.log = rf.log[:rf.commitIndex+1]
		rf.persist()
		copy(rf.log, tmp_log)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			// // fmt.Print("Server ", rf.me, " is applying command ", rf.log[i].Command, " at index ", i, "\n")
			msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i+1}
			rf.applyCh <- msg
		}
	
		
		rf.lastApplied = rf.commitIndex
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Term: %d, Server %d received AppendEntries from %d\n server log: %v", args.Term, rf.me, args.LeaderId, rf.log)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else
	{

		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isLeader = false
		rf.timeOut = false




		if args.PrevLogIndex == -1 {
			reply.Success = true


			rf.log = args.Entries

			return
		}
		if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return
		}

		reply.Success = true
		if args.Entries != nil {
			rf.log = rf.log[:args.PrevLogIndex+1]
			rf.log = append(rf.log, args.Entries...)
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
			go rf.updateStateMachine()
		}
		// if args.Entries == nil {
		// 	// fmt.Println("Server ", rf.me, "commitindex" , len(rf.log)-1, "leadercommit", args.LeaderCommit, "min", int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1))))
		// 	// if args.PrevLogTerm == 1{
		// 		// if args.LeaderCommit > rf.commitIndex {
		// 		// 	rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		// 		// 	go rf.updateStateMachine()
		// 		// }
		// 	// }
		// 	return
		// }
		

	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("Term: %d, Server %d is sending AppendEntries to %d\n Server log: %v", rf.currentTerm, rf.me, server, rf.log)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.isLeader = false

			return ok
		}
		if reply.Success {
			if args.Entries == nil {
				return ok
			}
			// DPrintf("Term: %d, Server %d accepted AppendEntries from %d\n", rf.currentTerm, rf.me, server)
			rf.nextIndex[server] = len(args.Entries) + args.PrevLogIndex + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		} else {
			rf.nextIndex[server]--
			//TODO: optimize the backoff
		}


	}
	return ok

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

func (rf *Raft) MoreUpToDate(term1 int, index1 int, term2 int, index2 int) bool {
	if term1 > term2 {
		return true
	}
	if term1 == term2 && index1 >= index2 {
		return true
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Term: %d, Server %d received RequestVote from %d\n", args.Term, rf.me, args.CandidateId)
	if len(rf.log) != 0 {
		DPrintf("term1: %d, index1: %d, term2: %d, index2: %d\n", args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log)-1)
	}
	//TODO not >= 
	if args.Term >= rf.currentTerm && (len(rf.log) == 0 ||
	rf.MoreUpToDate(args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log)-1)) {
		DPrintf("Term:%d, Server %d voted for %d\n", args.Term, rf.me, args.CandidateId)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.isLeader = false
		rf.timeOut = false

		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.isLeader = false
		}
		if reply.VoteGranted {
			// DPrintf("Term: %d, Server %d received vote from %d\n", rf.currentTerm, rf.me, server)
			rf.voteCount++
			if rf.voteCount >= int(math.Floor(float64(len(rf.peers))/2)) + 1 {
				rf.isLeader = true
				rf.nextIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
				}
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.matchIndex[i] = -1
				}
			}
		}
	}

		
	return ok
}



func (rf *Raft) updateCommitIndex() {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	kv := make(map[int]int)


	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			kv[rf.matchIndex[i]]++
		}
	}
	//sort the map by decreasing order
	var keys []int
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(keys)))
	presum := 0
	for _, k := range keys {
		presum += kv[k]
		// DPrintf("Server %d has %d matchIndex %d\n", rf.me, kv[k], k)
		// fmt.Println("Server", rf.me, "has", kv[k], "matchIndex", k)
		if k > rf.commitIndex && presum>= int(math.Floor(float64(len(rf.peers))/2)) {
			// DPrintf("Server %d is updating commitIndex to %d\n", rf.me, k)
			rf.commitIndex = k
			
			break
		}
	}

	

	

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
	if rf.isLeader {
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command, len(rf.log)})
		return len(rf.log), rf.currentTerm, true
	} 
	return -1, -1, false
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

func (rf *Raft) leadElection() {
	for !rf.killed() {
		rf.mu.Lock()


		
		// Your code here (3A)
		// Check if a leader election should be started.
		// If so, start the election.
		if rf.timeOut && !rf.isLeader{
			rf.voteCount = 1
			rf.currentTerm++
			rf.votedFor = rf.me
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					if len(rf.log) == 0 {
						go rf.sendRequestVote(i, &RequestVoteArgs{rf.currentTerm, rf.me, 0, 0}, &RequestVoteReply{})
					} else {
						go rf.sendRequestVote(i, &RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log)-1, rf.log[len(rf.log)-1].Term}, &RequestVoteReply{})
					}
				}
			}
		}


		rf.timeOut = true
		rf.mu.Unlock()
		// pause for a random amount of time between 200 and 500
		// milliseconds.

		ms := 500 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) heartBeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.isLeader {
			
			go rf.updateCommitIndex()
			go rf.updateStateMachine()

			
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					if len(rf.log) - 1 >= rf.nextIndex[i] {
						DPrintf("Server %d is sending AppendEntries to %d\n", rf.me, i)
						if rf.nextIndex[i] == 0 {
							go rf.sendAppendEntries(i, &AppendEntriesArgs{rf.currentTerm, rf.me, -1, -1, rf.log, rf.commitIndex}, &AppendEntriesReply{})
						} else {
							go rf.sendAppendEntries(i, &AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[i]-1, rf.log[rf.nextIndex[i]-1].Term, rf.log[rf.nextIndex[i]:], rf.commitIndex}, &AppendEntriesReply{})
						}
					} else {
						DPrintf("Server %d is sending heartbeats to %d\n", rf.me, i)
						// if rf.matchIndex[i] >= rf.commitIndex {
							// tell client to update commitIndex
							// go rf.sendAppendEntries(i, &AppendEntriesArgs{rf.currentTerm, rf.me, -1, 1, nil, rf.commitIndex}, &AppendEntriesReply{})
						// } else {
							if rf.nextIndex[i] == 0 {
								go rf.sendAppendEntries(i, &AppendEntriesArgs{rf.currentTerm, rf.me, -1, -1, nil, rf.commitIndex}, &AppendEntriesReply{})
							} else {
								go rf.sendAppendEntries(i, &AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[i]-1, rf.log[rf.nextIndex[i]-1].Term, nil, rf.commitIndex}, &AppendEntriesReply{})
							}
						// }
					}
		
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
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
	rf := &Raft{}
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	rf.isLeader = false
	rf.isCandidate = false
	rf.timeOut = false
	rf.voteCount = 0
	rf.applyCh = applyCh

	rf.commitIndex = -1
	rf.lastApplied = -1


	




	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.leadElection()

	go rf.heartBeat()


	return rf
}
