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
	//"os"
	"log"
	"time"
	"sync"
	"kvdb/labrpc"
	"math/rand"
)

// import "bytes"
// import "labgob"


const HeartBeatInterval = 80 * time.Millisecond
const CommitApplyIdleCheckInterval = 10 * time.Millisecond
const LeaderPeerTickInterval = 10 * time.Millisecond

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

type ServerState string
const (
	Follower  ServerState = "Follower"
	Candidate             = "Candidate"
	Leader                = "Leader"
)

type Log struct {
	Cmd interface{}
	Term int
}

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
	leaderID int
	term int
	votedFor int	

	lastHeartBeat time.Time
	state ServerState
	timeout time.Duration

	commitIndex int
	lastApplied int

	log []Log
	nextIndex []int
	matchIndex []int
}

func (rf *Raft) debug(format string, a ...interface{}) {
	args := append([]interface{}{rf.me, rf.term, rf.state}, a...)
	log.Printf("[INFO] Raft:[Id:%d|Term:%d|State:%s|] " + format, args...)
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) UnLock() {
	rf.mu.Unlock()
}

func (rf *Raft) isLeader() bool {
	return rf.state == Leader
}

func (rf *Raft) turnToFollow() {
	rf.state = Follower
	rf.votedFor = -1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.term, rf.isLeader()
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
	VoteGranted bool
}

func (rf *Raft) checkIfLogUpdateToDate(lastLogIndex int, lastLogTerm int) bool {
	if len(rf.log) > lastLogIndex {
		return false
	} else if len(rf.log) == lastLogIndex {
		_,term := rf.getLastLogEntry()
		if term != lastLogTerm {
			rf.debug("log length is the same, but the term is different, vote false")
			return false
		}
	}
	return true;
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//rf.debug("server %d requires me to vote, your term is %d", args.CandidateId, args.Term)
	reply.Term = rf.term

	// check if log is up-to-date
	updateToDate := rf.checkIfLogUpdateToDate(args.LastLogIndex, args.LastLogTerm)

	if args.Term < rf.term {
		reply.VoteGranted = false		
	} else if args.Term >= rf.term && updateToDate{
		rf.turnToFollow()
		rf.term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeartBeat = time.Now()
	} else if rf.votedFor == -1 || args.CandidateId == rf.votedFor {
		if updateToDate {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastHeartBeat = time.Now()
		}		
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int
	// entries
	Entries []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	//rf.debug("receive append entry from leader %d", args.LeaderId)
	reply.Term = rf.term
	if rf.isLeader() {
		//rf.debug("I am a leader, why some dude gives me entry??????? %d", args.Term)
	}
	if args.Term < rf.term {
		reply.Success = false
		return
	} else if args.Term >= rf.term {
		rf.turnToFollow()
		rf.term = args.Term
		rf.leaderID = args.LeaderId
	}

	if rf.leaderID == args.LeaderId {
		rf.lastHeartBeat = time.Now()
	}

	log.Println(rf.me, "receives", args, "my logs are", rf.log)

	// reply false if log not contain an entry at preLogIndex
	if args.PreLogIndex > len(rf.log) {
		reply.Success = false
		return
	} else if args.PreLogIndex-1 >= 0 && rf.log[args.PreLogIndex-1].Term != args.PreLogTerm {
		reply.Success = false
		return
	}
	
	reply.Success = true	
	// TODO for now just one entry log
	if len(args.Entries) > 0 {
		// if an existing entry conflicts with a new one
		// same index, different terms, delete existing entry and all that follow it
		if len(rf.log) > args.PreLogIndex {
			e := rf.log[args.PreLogIndex]
			if e.Term != args.Entries[0].Term {
				log.Println("mushroom before", rf.log, "args:", args)
				rf.log = rf.log[:args.PreLogIndex]
				log.Println("mushroom after", rf.log)
			} else if e.Cmd == args.Entries[0].Cmd {
				log.Println("mushroom existing entry", rf.log)
				rf.log = rf.log[:args.PreLogIndex]
			}
		}		

		rf.log = append(rf.log, args.Entries[0])
	}	
	
	log.Println(rf.me, "request leader commit index is", args.LeaderCommit, "my commit index", rf.commitIndex, rf.log)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
	}
	log.Println(rf.me, "commit index", rf.commitIndex)
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

func (rf *Raft) appendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, reply)
	return ok
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
	if rf.isLeader() == false {
		return -1, -1, false
	}

	entry := Log{
		Cmd : command,
		Term : rf.term,
	}
	rf.log = append(rf.log, entry)
	//rf.debug("%v", rf.log)

	//rf.debug("add command %v", command)
	return len(rf.log), rf.term, true
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

func (rf *Raft) updateCommitIndex() {
	i := len(rf.log)
	for i > 0 {
		v := rf.log[i - 1]
		count := 1
		//log.Println(v, "i is", i, "commit index", rf.commitIndex, "every body matches", rf.matchIndex, "v term", v.Term, "my cur term", rf.term)
		if v.Term == rf.term && i > rf.commitIndex {
			// check if has majority
			// Note: this j the value, not index
			for serverIndex, j := range rf.matchIndex {
				if serverIndex == rf.me {
					continue
				}
				// for any matchIndex value (j), if larger or equal to i (the log index starting from last), incre count.
				if j >= i { 
					count++
				}
			}
		}
		//log.Println(rf.me, "count is", count)
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			log.Println(rf.me, "peer got commit index", rf.commitIndex, "count is", count, "peer num is", len(rf.peers)/2)
			break
		}
		i--
	}
	//log.Println(rf.me, "match indexes are", rf.matchIndex)
}

func (rf *Raft) sendAppendEntries(s int) {			
	nIndex := rf.nextIndex[s]
	entries := []Log{}
	if len(rf.log) > 0 && len(rf.log) >= nIndex {
		// we have the next log entry, so send it	
		sendLog := Log {
			Cmd : rf.log[nIndex-1].Cmd,
			Term : rf.log[nIndex-1].Term,
		}
		entries = append(entries, sendLog)
		log.Println(rf.me, "sending entries", entries, "to", s, "my log", rf.log, "next index", nIndex, "length", len(rf.log))
	}
		
	preIndex := rf.matchIndex[s]	
	preTerm := 0
	if len(rf.log) > 0 && len(rf.log) >= preIndex {
		preLogIndex := preIndex -1
		if preLogIndex < 0 {
			preLogIndex = 0
		}
		preTerm = rf.log[preLogIndex].Term
	} else {
		log.Println(rf.me, "mama sending entries", entries, "to", s, "my log", rf.log, "pre index", preIndex)
	}
	
	args := AppendEntriesArgs {
		Term : rf.term,
		LeaderId : rf.me,
		PreLogIndex : preIndex,
		PreLogTerm : preTerm,
		Entries : entries,
		LeaderCommit : rf.commitIndex,
	}	
	
	reply := &AppendEntriesReply {}
	ok := rf.appendEntries(s, args, reply)
	if ok {
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.turnToFollow()
			return // we are out
		}

		if reply.Success {
			// update log
			if len(entries) > 0 {
				// this is to prevent duplicate packet being received. Only incre if matched.
				if rf.matchIndex[s] == preIndex { 
					rf.nextIndex[s]++
					rf.matchIndex[s]++
				}							
			}	
		} else {
			rf.debug("reduce next index for server %d", s)
			rf.nextIndex[s]--
			if rf.nextIndex[s] < 1 {
				rf.nextIndex[s] = 1 // the min is 1
			}
		}
	}
	rf.updateCommitIndex()
}

func (rf *Raft) appendEntriesLoopForPeer(server int) {
	ticker := time.NewTicker(LeaderPeerTickInterval)

	rf.sendAppendEntries(server)
	lastEntrySent := time.Now()

	for {
		rf.Lock()
		if rf.state != Leader  {
			ticker.Stop()
			rf.UnLock()
			break
		}
		rf.UnLock()

		select {
		case currentTime := <-ticker.C: 
			if currentTime.Sub(lastEntrySent) >= HeartBeatInterval {
				lastEntrySent = time.Now()
				// we fire and contine, otherwise the loop stuck if the peer no responding
				go rf.sendAppendEntries(server)
			}
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.leaderID = rf.me
	rf.debug("I am a leader!")

	for p := range rf.peers {
		if p == rf.me {
			continue
		}	
		go rf.appendEntriesLoopForPeer(p)
	}
}

func (rf *Raft) getLastLogEntry() (int,int) {
	if len(rf.log) == 0 {
		return 0, 0
	}
	lastIndex := len(rf.log)
	v := rf.log[lastIndex-1]
	return lastIndex, v.Term
}

func (rf *Raft) beginElection() {
	//rf.debug("about to elect myself")
	rf.state = Candidate
	rf.term++
	rf.votedFor = rf.me	

	voteCount := 1
	cachedTerm := rf.term
	
	for s := range rf.peers {
		if s == rf.me {
			continue
		}	
		
		go func(serverIndex int) {
			index, term := rf.getLastLogEntry()
			req := &RequestVoteArgs{
				Term : cachedTerm,
				CandidateId : rf.me,
				LastLogIndex : index,
				LastLogTerm : term,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(serverIndex, req, reply)
			if ok {
				//rf.debug("receive from server %d term %d vote %t", serverIndex, reply.Term, reply.VoteGranted)

				if cachedTerm == rf.term { // only process in same term
					if reply.Term > rf.term {
						rf.term = reply.Term
						rf.turnToFollow()					
					} else if reply.VoteGranted {
						voteCount++
						if voteCount > len(rf.peers)/2 && rf.state == Candidate {
							rf.state = Leader
							go rf.becomeLeader()
						}
					}
				}
				
			} else {
				//rf.debug("why not ok for votes")
			}			
		}(s)
		
	}	
}

func (rf *Raft) startElectionProcess() {
	electionTimeout := func() time.Duration { // Randomized timeouts between [500, 600)-ms
		return (200 + time.Duration(rand.Intn(200))) * time.Millisecond
	}

	rf.timeout = electionTimeout()
	currentTime := <-time.After(rf.timeout)

	rf.Lock()
	defer rf.UnLock()

	//rf.debug("idling. am I leader? %t", rf.isLeader())
	// Start election process if we're not a leader and the haven't received a heartbeat for `electionTimeout`
	if rf.state != Leader && currentTime.Sub(rf.lastHeartBeat) >= rf.timeout {
		//log.Println(rf.me, "current time", currentTime, "last hearbeat", rf.lastHeartBeat)
		go rf.beginElection()
	}
	go rf.startElectionProcess()
}

func (rf *Raft) startLocalApplyProcess(applyChan chan ApplyMsg) {
	for {
		<-time.After(CommitApplyIdleCheckInterval)

		if rf.commitIndex > 0 && rf.commitIndex > rf.lastApplied {
			if rf.commitIndex - rf.lastApplied > 1 {
				log.Println(rf.me, "we are ready to send commit index", rf.commitIndex, rf.log, "last applied", rf.lastApplied)
			}

			// we need to fill the missing commit entries			
			nextLogIndex := rf.lastApplied // next log index we want to apply
			for nextLogIndex < rf.commitIndex {
				if rf.commitIndex - rf.lastApplied > 1 {
					log.Println(rf.me, "send index", nextLogIndex)
				}

				applyChan <- ApplyMsg {
					CommandIndex:nextLogIndex + 1, // index starts from 1. log index starts from 0
					Command:rf.log[nextLogIndex].Cmd,
					CommandValid : true,
				}				
				
				nextLogIndex++
			}
			
			rf.lastApplied = rf.commitIndex
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
	rf.state = Follower
	rf.term = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = []Log{}

	rf.nextIndex = make([]int, len(rf.peers))
	// initialized to leader last log index + 1
	logIndexInit := len(rf.log) + 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = logIndexInit
	}
	rf.matchIndex = make([]int, len(rf.peers)) // init to 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.startElectionProcess()
	go rf.startLocalApplyProcess(applyCh)

	return rf
}
