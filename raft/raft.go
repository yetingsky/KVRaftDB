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
	"kvdb/labgob"
	"bytes"
	"time"
	"sync"
	"kvdb/labrpc"
	"math/rand"
)


const HeartBeatInterval = 60 * time.Millisecond
const CommitApplyIdleCheckInterval = 15 * time.Millisecond
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
	Index int
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

	isDecommissioned bool

	sendAppendChan []chan struct{}
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
	rf.persist()
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
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
	currentTerm := 0
	votedFor := -1
	logs := []Log{}
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil || 
	   d.Decode(&logs) != nil {
	   //log.Println("Something bad is happening in decoder!")
	} else {
	  rf.term = currentTerm
	  rf.votedFor = votedFor
	  rf.log = logs
	}
	rf.persist()
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
	lastI, lastT := rf.getLastLogEntry()
	if lastT == lastLogTerm {
		return lastI <= lastLogIndex
	} else {
		return lastT < lastLogTerm
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//rf.debug("server %d requires me to vote, your term is %d", args.CandidateId, args.Term)

	rf.Lock()
	defer rf.UnLock()

	reply.Term = rf.term

	// check if log is up-to-date
	updateToDate := rf.checkIfLogUpdateToDate(args.LastLogIndex, args.LastLogTerm)

	if args.Term < rf.term {
		reply.VoteGranted = false		
	} else if args.Term >= rf.term && updateToDate {
		// of course I will vote for you no matter what
		rf.turnToFollow()
		rf.term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else if (rf.votedFor == -1 || args.CandidateId == rf.votedFor) && updateToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	log.Println(rf.me, "got vote ask", args, "updateTodate?", updateToDate, "my term is", rf.term, "my voted is to", rf.votedFor, "did I vote?", reply.VoteGranted, rf.log)
	rf.persist()
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
	ConflictingLogTerm int
	ConflictingLogIndex int
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
	rf.Lock()
	defer rf.UnLock()

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

	// find previous index
	// Note: preIndex is -1 in two cases:
	// 1. No match. Reply false!
	// 2. args.prev is the beginning of log, we need to process
	preIndex := -1
	for i, v := range rf.log {
		if v.Index == args.PreLogIndex {
			if v.Term == args.PreLogTerm {				
				preIndex = i			
				break
			} else {
				// now we have the conflict term
				reply.ConflictingLogTerm = v.Term
			}
		}
	}

	PrevIsBeginningOfLog := args.PreLogIndex == 0 && args.PreLogTerm == 0
	// compare start from preIndex+1
	// so if we found it, or prev is the beginning, we process
	if preIndex >= 0 || PrevIsBeginningOfLog {
		entryIndex := 0
		for i := preIndex + 1; i < len(rf.log); i++ {
			entryConsistent := func() bool {
				localEntry, argsEntry := rf.log[i], args.Entries[entryIndex]
				return (localEntry.Term == argsEntry.Term) && (localEntry.Index == argsEntry.Index)
			}

			if entryIndex >= len(args.Entries) || !entryConsistent() {
				// delete extra inconsistent entries, exclude the current i since it is inconsistent
				rf.log = rf.log[:i]
				break;
			} else {
				// test code
				if rf.log[i].Cmd != args.Entries[entryIndex].Cmd {
					//log.Println("what what?")
					rf.log[i].Cmd = args.Entries[entryIndex].Cmd
				}				
				
				entryIndex++
			}
		}

		// append the rest of NEW entries, starting from entryIndex
		if entryIndex < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[entryIndex:]...)
		}

		log.Println(rf.me, "request leader commit index is", args.LeaderCommit, "my commit index", rf.commitIndex, rf.log)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		}
		log.Println(rf.me, "commit index", rf.commitIndex)
		reply.Success = true
	} else {
		// When rejecting AppendEntries, followers include the term of the conflicting entry,
		// and the first index it stores for that term
		if reply.ConflictingLogTerm == 0 && len(rf.log) > 0 {
			reply.ConflictingLogTerm = rf.log[len(rf.log)-1].Term
		}

		for _,v := range rf.log {
			// get the conflict index for the conflict term
			if v.Term == reply.ConflictingLogTerm {
				reply.ConflictingLogIndex = v.Index
				break
			}
		}

		reply.Success = false
	}
	rf.persist()
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

	rf.Lock()
	defer rf.UnLock()
	entry := Log{
		Cmd : command,
		Term : rf.term,
		Index : len(rf.log) + 1,
	}
	rf.log = append(rf.log, entry)
	
	rf.persist()

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
	rf.Lock()
	defer rf.UnLock()

	rf.isDecommissioned = true
}

func (rf *Raft) updateCommitIndex() {
	rf.Lock()
	defer rf.UnLock()

	i := len(rf.log)
	for i > 0 {
		v := rf.log[i - 1]
		count := 1
		//log.Println(v, "i is", i, "commit index", rf.commitIndex, "every body matches", rf.matchIndex, "v term", v.Term, "my cur term", rf.term)

		// a leader cannot determine commitment using log entries from older terms
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
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			log.Println(rf.me, "peer got commit index", rf.commitIndex, "count is", count, "peer num is", len(rf.peers)/2)
			break
		}
		i--
	}
}

func (rf *Raft) sendAppendEntries(s int, sendAppendChan chan struct{}) {
	if !rf.isLeader() || rf.isDecommissioned {
		return
	}

	entries := []Log{}
	preLogIndex := 0
	preLogTerm := 0
	lastLogIndex,_ := rf.getLastLogEntry()

	rf.Lock()
	// if we have log entry, and our logs contain nextIndex[s]
	if lastLogIndex > 0 && lastLogIndex >= rf.nextIndex[s] {
		// send all missing entries!
		for i,_ := range rf.log {
			// current i plus 1 is the log index. Only proceed if we are currently at next index for peer
			if i + 1 == rf.nextIndex[s] {
				if i > 0 {
					// get previous index's log index.
					preLogIndex = (i - 1) + 1
					preLogTerm = rf.log[i - 1].Term
				}
				// Note: length of log minus previous log index is len(rf.log)-i
				// the current one we need to send, so from current to the end
				entries = make([]Log, len(rf.log) - preLogIndex)
				copy(entries, rf.log[i:])
				//log.Println(rf.me, "mama sending entries", entries, "to", s, "my log", rf.log, "pre index", preLogIndex)
				break
			}
		}
	} else {
		if len(rf.log) > 0 {
			// nextIndex larger than our last log index, we send heartbeat since we assume the peer is up-to-date
			// if it is not, then they will reply false back to us, so we decrement
			preLogIndex, preLogTerm = rf.getLastLogEntry()
			//log.Println(rf.me, "last log index term are", preLogIndex, preLogTerm)
		}		
	}
	
	args := AppendEntriesArgs {
		Term : rf.term,
		LeaderId : rf.me,
		PreLogIndex : preLogIndex,
		PreLogTerm : preLogTerm,
		Entries : entries,
		LeaderCommit : rf.commitIndex,
	}	
	log.Println(rf.me, "send append entries args to peer", s, "args are:", args, "my logs", rf.log)

	rf.UnLock()

	reply := &AppendEntriesReply {}
	ok := rf.appendEntries(s, args, reply)
	if ok {
		if rf.state != Leader || rf.isDecommissioned || args.Term != rf.term {
			//rf.debug("I am not leader anymore, discard response!")
			return
		}

		if reply.Term > rf.term {
			rf.Lock()
			rf.term = reply.Term
			rf.turnToFollow()
			rf.persist()
			rf.UnLock()
			return // we are out
		}

		if reply.Success {
			// update log
			if len(entries) > 0 {
				rf.matchIndex[s] = preLogIndex + len(entries)
				rf.nextIndex[s] = rf.matchIndex[s] + 1
			}	
		} else {
			//rf.debug("reduce next index for server %d", s)
			// go back to conflict log index minus one, so we are safe. but the lowest index is 1.
			max := reply.ConflictingLogIndex-1
			if max < 1 {
				max = 1
			}
			rf.nextIndex[s] = max
			//log.Println(rf.me, "all next indexes are", rf.nextIndex, "my logs", rf.log)

			sendAppendChan <- struct{}{}
		}

		rf.updateCommitIndex()
	}	
	rf.persist()
}

func (rf *Raft) appendEntriesLoopForPeer(server int, sendAppendChan chan struct{}) {
	if rf.isDecommissioned {
		return
	}
	ticker := time.NewTicker(LeaderPeerTickInterval)

	rf.sendAppendEntries(server, sendAppendChan)
	lastEntrySent := time.Now()

	for {
		rf.Lock()
		if rf.state != Leader || rf.isDecommissioned {
			ticker.Stop()
			rf.UnLock()
			break
		}
		rf.UnLock()

		select {
		case <-sendAppendChan: // Signal that we should send a new append to this peer
			lastEntrySent = time.Now()
			go rf.sendAppendEntries(server, sendAppendChan)
		case currentTime := <-ticker.C: 
			if currentTime.Sub(lastEntrySent) >= HeartBeatInterval {
				lastEntrySent = time.Now()
				// we fire and contine, otherwise the loop stuck if the peer no responding
				go rf.sendAppendEntries(server, sendAppendChan)
			}
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.leaderID = rf.me
	rf.debug("I am a leader!")

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.sendAppendChan = make([]chan struct{}, len(rf.peers))

	for p := range rf.peers {
		if p == rf.me {
			continue
		}
		
		rf.nextIndex[p] = len(rf.log) + 1 // Should be initialized to leader's last log index + 1
		rf.matchIndex[p] = 0              // Index of highest log entry known to be replicated on server
		rf.sendAppendChan[p] = make(chan struct{}, 1)
		
		go rf.appendEntriesLoopForPeer(p, rf.sendAppendChan[p])
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
	rf.Lock()
	rf.state = Candidate
	rf.term++
	rf.votedFor = rf.me	

	voteCount := 1
	cachedTerm := rf.term

	rf.persist()
	rf.UnLock()

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
			log.Println(rf.me, "hi vote for me", req)
			ok := rf.sendRequestVote(serverIndex, req, reply)
			if ok {
				rf.debug("receive from server %d term %d vote %t", serverIndex, reply.Term, reply.VoteGranted)

				if cachedTerm == rf.term { // only process in same term
					if reply.Term > rf.term {
						rf.Lock()
						rf.term = reply.Term
						rf.turnToFollow()
						rf.persist()		
						rf.UnLock()		
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
		return (200 + time.Duration(rand.Intn(300))) * time.Millisecond
	}

	rf.timeout = electionTimeout()
	currentTime := <-time.After(rf.timeout)

	rf.Lock()
	defer rf.UnLock()

	if !rf.isDecommissioned {
		// Start election process if we're not a leader and the haven't received a heartbeat for `electionTimeout`
		if rf.state != Leader && currentTime.Sub(rf.lastHeartBeat) >= rf.timeout {
			go rf.beginElection()
		}
		go rf.startElectionProcess()
	}	
}

func (rf *Raft) startLocalApplyProcess(applyChan chan ApplyMsg) {
	for {
		if rf.commitIndex >= 0 && rf.commitIndex > rf.lastApplied && !rf.isDecommissioned {
			if rf.commitIndex - rf.lastApplied > 1 {
				//log.Println(rf.me, "we are ready to send commit index", rf.commitIndex, rf.log, "last applied", rf.lastApplied)
			}

			// we need to fill the missing commit entries			
			nextLogIndex := rf.lastApplied // next log index we want to apply
			cachedCommitIndex := rf.commitIndex
			for nextLogIndex < cachedCommitIndex {
				if cachedCommitIndex - rf.lastApplied > 1 {
					//log.Println(rf.me, "send index", nextLogIndex)
				}

				applyChan <- ApplyMsg {
					CommandIndex:nextLogIndex + 1, // index starts from 1. log index starts from 0
					Command:rf.log[nextLogIndex].Cmd,
					CommandValid : true,
				}				
				
				nextLogIndex++
			}
			
			rf.Lock()
			rf.lastApplied = cachedCommitIndex
			rf.UnLock()
		} else {
			<-time.After(CommitApplyIdleCheckInterval)
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionProcess()
	go rf.startLocalApplyProcess(applyCh)

	return rf
}
