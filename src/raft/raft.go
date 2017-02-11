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
	"sync"
	"labrpc"
	"math/rand"
	"time"
	"fmt"
	"math"
	"bytes"
	"encoding/gob"
	"strings"
)

type State int
const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

func (rf *Raft) stateToString() string {
	switch rf.state {
	case LEADER: 
		return "Leader"
	case FOLLOWER: 
		return "Follower"
	case CANDIDATE: 
		return "Candidate"
	default:
		return ""
	}
}

func (rf *Raft) toString() string {
	return fmt.Sprintf("<Peer:%d Term:%d State:%s>", rf.me, rf.CurrentTerm, rf.stateToString())
}

func logDebug(msg string) {
	if DEBUG { fmt.Printf(msg) }
}

func logLockDebug(msg string){
	if LOCK_DEBUG { fmt.Printf(msg) }
}

const ELECTION_TIMEOUT_MIN = 150
const ELECTION_TIMEOUT_MAX = 300
const HEARTBEAT_TIMEOUT = 40
const APPLY_STATE_TIMEOUT = 50
const DEBUG = true
const LOCK_DEBUG = false

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex
	//THREAD-SAFE
	peers       []*labrpc.ClientEnd //immutable
	persister   *Persister
	me          int // index into peers[], immutable
	peerNums    []int //the indices of all peers to this raft
	//LOCK BEFORE READ OR WRITE
	state       State //if this Raft thinks its the leader
	CurrentTerm int //last term this raft has seen (starts at 0)
	VotedFor    int //peer that this raft voted for in last term
	Logs        []LogEntry //log entries (indexed 0 - N)
	CommitIndex int //index of highest log entry known to be commited, (indexed 0 - N, initialized to -1)
	lastApplied int //index of highest log entry applied to state machine (indexed 0 - N, initialized to -1)
	VotesFor    int
	followerCh  chan bool
	candidateCh chan bool
	leaderCh    chan bool
	//Leaders only
    NextIndex     []int //for each server, index of next log entry to send to that server (indexed 0-N, initialized to 0)
	matchIndex    []int //for each server, index of highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := rf.CurrentTerm
	isLeader := (rf.state == LEADER)
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:RequestVote() HAS the Lock\n", rf.toString())) }

	lastLogIndex := len(rf.Logs) - 1
	var lastLogTerm int
	if lastLogIndex > -1 {
		lastLogTerm = rf.Logs[lastLogIndex].Term 
	} else {
		lastLogTerm = 0
	}	

	// If the requester's term is less than this peer's term: VoteGranted=false
	if args.Term < rf.CurrentTerm {
		logDebug(fmt.Sprintf("%s:DENIED RequestVote from Requester:<Peer:%d Term:%d> (Requester Behind This Peer's Term)\n", 
				rf.toString(), args.CandidateId, args.Term))

		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	} else if args.Term == rf.CurrentTerm {
	// If the requester's term and this peers term are the same...
		// If this Peer has already voted or the Requester's log is behind this Peer's log VoteGranted=false
		//  A requester's log is behind this peer's log if:
		//   1) The lastLogTerm of the requester is less than the lastLogTerm of this peer
		//   2) The lastLogTerms are equal and the lastLogIndex of the requester is less than the LastLogIndex of this peer
		if rf.VotedFor > -1 || args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			logDebug(fmt.Sprintf("%s:DENIED RequestVote from Requester:<Peer:%d Term:%d> (This Peer Already Voted OR Requester's Logs Are Behind)\n", 
				rf.toString(), args.CandidateId, args.Term))

			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false 
		} else {
		// If the Requester's log is not behind this Peer's log VoteGranted=true
			logDebug(fmt.Sprintf("%s:GRANTED RequestVote from Requester:<Peer:%d Term:%d>\n", 
					rf.toString(), args.CandidateId, args.Term))

			rf.VotedFor = args.CandidateId
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = true
		}
	} else {
	// If Requester's term is greater than this Peer's current term
		// If the Requester's log is behind this Peer's log VoteGranted=false
		//  A requester's log is behind this peer's log if:
		//   1) The lastLogTerm of the requester is less than the lastLogTerm of this peer
		//   2) The lastLogTerms are equal and the lastLogIndex of the requester is less than the LastLogIndex of this peer		
		if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			logDebug(fmt.Sprintf("%s:DENIED RequestVote from Requester:<Peer:%d Term:%d> (Requesters Logs Are Behind)\n", 
					rf.toString(), args.CandidateId, args.Term))

			reply.VoteGranted = false
		} else {
		// If the Requester's logs are not behind this Peer's logs VoteGranted=true
			logDebug(fmt.Sprintf("%s:GRANTED RequestVote from Requester:<Peer:%d Term:%d>\n", 
					rf.toString(), args.CandidateId, args.Term))

			rf.VotedFor = args.CandidateId
			reply.VoteGranted = true
		}

		rf.CurrentTerm = args.Term
		reply.Term = rf.CurrentTerm
		rf.followerCh <- true
	}
	rf.mu.Unlock()
	if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:RequestVote() RELEASES the Lock\n", rf.toString())) }
}

// Send requestVote RPC's to all peers
func (rf *Raft) broadcastRequestVote(){
	logDebug(fmt.Sprintf("%s:Sending RequestVote to Peers:%v\n", rf.toString(), rf.peerNums))

	for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
		rf.mu.Lock()
		if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:broadcastRequestVote() HAS the Lock\n", rf.toString())) }

		if peerNum != rf.me {
			args := &RequestVoteArgs{}
			args.Term = rf.CurrentTerm
			args.CandidateId = rf.me
			lastLogIndex := len(rf.Logs) - 1
			args.LastLogIndex = lastLogIndex
			if lastLogIndex < 0 {
				args.LastLogTerm = 0
			} else {
				args.LastLogTerm = rf.Logs[lastLogIndex].Term
			}

			reply := &RequestVoteReply{}

			go rf.sendRequestVote(peerNum, *args, reply)
		}

		rf.mu.Unlock()
		if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:broadcastRequestVote() RELEASES the Lock\n", rf.toString())) }
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:sendRequestVote() HAS the Lock\n", rf.toString())) }

		// If this Peer is still a candidate and got the vote
		if rf.state == CANDIDATE && reply.VoteGranted {
			rf.VotesFor += 1
			// If this Candidate has a majority of votes in this election
			if (rf.VotesFor * 2) > len(rf.peers) {
				//Let the main thread know that you have enough votes to be leader
				select {
				case rf.leaderCh <- true:
				default:
					// Dont fill the channel if it's already full (leads to deadlock)
				}	
			}
		}

		rf.mu.Unlock()
		if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:sendRequestVote() RELEASES the Lock\n", rf.toString())) }

	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int //Leaders term
	LeaderId     int //Leaders id in peers[]
	PrevLogIndex int //Index of last log entry 
	PrevLogTerm  int //Term of last log entry
	Entries      []LogEntry //Entries to store (empty for heartbeat)
	LeaderCommit int //Leaders commitIndex
}

type AppendEntriesReply struct {
	Term    int //current term 
	Success bool
	MatchIndex int //index of the last LogEntry on this server (only relevant if you get successful return)
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:AppendEntries() HAS the Lock\n", rf.toString())) }

	// If the Leader's term is less than this Peer's term Success=false
	if args.Term < rf.CurrentTerm {
		logDebug(fmt.Sprintf("%s:Got AppendEntry for earlier term from:<Leader:%d Term:%d>\n", 
				rf.toString(), args.LeaderId, args.Term))

		reply.Term = rf.CurrentTerm
		reply.MatchIndex = len(rf.Logs) - 1
		reply.Success = false
	} else {
	// Otherwise Success=true
		// If this Peer's term is less than the Leader's term, update it
		if rf.CurrentTerm < args.Term {
			rf.CurrentTerm = args.Term
		}
		reply.Term = rf.CurrentTerm

		lastLogIndex := len(rf.Logs) - 1
		var lastLogTerm int
		if lastLogIndex > -1 {
			lastLogTerm = rf.Logs[lastLogIndex].Term 
		} else {
			lastLogTerm = 0
		}	
		// If this Peer's lastLogIndex and lastLogTerm is the same as the Leader's lastLogIndex and lastLogTerm
		if args.PrevLogIndex == lastLogIndex && args.PrevLogTerm == lastLogTerm {
			// if len(args.Entries) > 0 { 
			// 	logDebug(fmt.Sprintf("%s:Got AppendEntry from <Leader:%d, Term:%d> with MATCHING lastLogIndex:(Leader:%d/Peer:%d) and lastTerm:(Leader:%d/Peer:%d)\n", 
			// 		rf.toString(), args.LeaderId, args.Term, args.PrevLogIndex, lastLogIndex, args.PrevLogTerm, lastLogTerm)) 
			// }

			//If the Leader has more comitted entries than this Peer, update this Peer's CommitIndex 
			if rf.CommitIndex < args.LeaderCommit {
				rf.CommitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.Logs) - 1)))
			}
			//Persist the new commits
			go rf.persist()

			//Add all new log entries to this Peer
			if len(args.Entries) > 0 {
				logDebug(fmt.Sprintf("%s:Adding Log Entries[%d-]:%v\n", rf.toString(), lastLogIndex + 1, args.Entries))

				rf.Logs = append(rf.Logs, args.Entries...)
			}
			
			reply.MatchIndex = len(rf.Logs) - 1
			reply.Success = true
		} else {
		// If this Peer's lastLogIndex or lastLogTerm don't match the Leader's
			// logDebug(fmt.Sprintf("%s:Got AppendEntry from:<Leader:%d, Term:%d> with MISMATCHED lastLogIndex:(Leader:%d/Peer:%d) or lastTerm:(Leader:%d/Peer:%d)\n", 
			// 		rf.toString(), args.LeaderId, args.Term, args.PrevLogIndex, lastLogIndex, args.PrevLogTerm, lastLogTerm))

			// If this Peer's log has more entries than the Leader's log, delete the extra entries from this peer
			if lastLogIndex > args.PrevLogIndex {
				origLogLength := len(rf.Logs)
				rf.Logs = rf.Logs[:(args.PrevLogIndex + 1)]
				reply.MatchIndex = len(rf.Logs) - 1

				logDebug(fmt.Sprintf("%s:lastLogIndex:(L:%d/P:%d) is ahead of:<Leader:%d, Term:%d>... DELETING %d entries from this Peer's log\n", 
					rf.toString(), args.PrevLogIndex, lastLogIndex, args.LeaderId, args.Term, origLogLength - len(rf.Logs)))
			} else if lastLogIndex == args.PrevLogIndex || lastLogTerm != args.PrevLogTerm {
			// If the lastLogIndexes are the same length but this Peer's lastLogTerm is not equal to the Leader's lastLogTerm
				logDebug(fmt.Sprintf("%s:lastLogIndexes match:<Leader:%d, Term:%d> but lastTerms don't:(L:%d/P:%d). DELETING 1 entry from this Peer's log\n", 
						rf.toString(), args.LeaderId, args.Term, args.PrevLogTerm, lastLogTerm))

				rf.Logs = rf.Logs[:lastLogIndex]
				reply.MatchIndex = lastLogIndex - 1
			} else {
			// If this Peer's lastLogIndex is behind the Leader, then just let the Leader know
				reply.MatchIndex = len(rf.Logs) - 1
				logDebug(fmt.Sprintf("%s:lastLogIndex:(Leader:%d/Peer:%d) is behind:<Leader:%d, Term:%d>\n", 
						rf.toString(), args.PrevLogIndex, len(rf.Logs) - 1, args.LeaderId, args.Term))
			}
			reply.Success = false
		}
		
		//Send the heartbeat notice to the main server thread
		rf.followerCh <- true
	}
	rf.mu.Unlock()
	if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:AppendEntries() RELEASES the Lock\n", rf.toString())) }
}

// Brooadcast AppendEntries RPCs to all peers
func (rf *Raft) broadcastAppendEntries(){
	msgs := make([]string, 0)
	for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
		if peerNum != rf.me {
			rf.mu.Lock()
			if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:broadcastAppendEntries() HAS the Lock\n", rf.toString())) }

			args := &AppendEntriesArgs{}
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			// Get the index of the next log entry to send to this Peer
			nextLogIdx := rf.NextIndex[peerNum]
			// Set the info of the previous log entry (immediately preceeding nextLogIdx)
			args.PrevLogIndex = nextLogIdx - 1
			if args.PrevLogIndex > -1 {
				args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term 
			} else {
				args.PrevLogTerm = 0
			}
			args.Entries = rf.Logs[nextLogIdx:]
			args.LeaderCommit = rf.CommitIndex

			reply := &AppendEntriesReply{}

			if len(args.Entries) > 0 {
				msgs = append(msgs, 
					fmt.Sprintf("<Peer:%d, Entries[%d-]:%v>", 
						peerNum, args.PrevLogIndex + 1, args.Entries))
			}

			rf.mu.Unlock()
			if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:broadcastAppendEntries() RELEASES the Lock\n", rf.toString())) }

			go rf.sendAppendEntries(peerNum, *args, reply)
		}		
	}

	if (len(msgs) > 0){
		logDebug(fmt.Sprintf("%s:Broadcasting new Log Entries to:%v\n", 
			rf.toString(), strings.Join(msgs, ", ")))
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:sendAppendEntries() HAS the Lock\n", rf.toString())) }

		if rf.state != LEADER {
			logDebug(fmt.Sprintf("%s:Not a leader, no longer accepting AppendEntry replies\n", 
					rf.toString()))

			rf.mu.Unlock()
			if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:sendAppendEntries() RELEASES the Lock\n", rf.toString())) }
			return ok
		}

		if !reply.Success {
			// If this Leader's term is less than the Peer's term, update its term and stand down to follower
			if rf.CurrentTerm < reply.Term {
				logDebug(fmt.Sprintf("%s:Found out it's behind (Term:%d)\n", 
						rf.toString(), reply.Term))

				rf.CurrentTerm = reply.Term
				rf.followerCh <- true
			} else {
			// Otherwise we need to update this Leader's MatchIndex and NextIndex arrays according to the MatchIndex returned by the Peer
				logDebug(fmt.Sprintf("%s:AppendEntry to <Peer:%d> was Not sucessful, changing MatchIndex[%d] to %d and NextIndex[%d] to %d\n", 
					rf.toString(), server, server, reply.MatchIndex, server, reply.MatchIndex + 1)) 

				rf.matchIndex[server] = reply.MatchIndex
				rf.NextIndex[server] = reply.MatchIndex + 1
			}	
		} else {
		// If the call was successful then update the leaders matchIndex array and NextIndex Array
			rf.matchIndex[server] = reply.MatchIndex
			rf.NextIndex[server] = reply.MatchIndex + 1
		}

		rf.mu.Unlock()
		if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:sendAppendEntries() RELEASES the Lock\n", rf.toString())) }

	}
	return ok
}

func (rf *Raft) commitNewEntries(){
	rf.mu.Lock()
	if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:commitNewEntries() HAS the Lock\n", rf.toString())) }

	if rf.state != LEADER || len(rf.Logs) == 0 {
		rf.mu.Unlock()
		if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:commitNewEntries() RELEASES the Lock\n", rf.toString())) }
		return 
	}
	countsLen := len(rf.Logs)
	// Array that holds the count of number of servers a LogEntry is replicated on
	counts := make([]int, countsLen)

	// Iterate through the Leader's matchIndex array and see how many Peers have LogEntries > rf.CommitIndex
	for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
		if peerNum != rf.me && rf.matchIndex[peerNum] > rf.CommitIndex {
			//Increment the counts array for entries > rf.CommitIndex and <= rf.matchIndex[peerNum]
			for countIdx := rf.CommitIndex + 1; countIdx <= rf.matchIndex[peerNum]; countIdx++ {
				counts[countIdx] += 1
			}
		}
	}

	oldCommit := rf.CommitIndex
	//If a majority of servers have a certain new LogEntry, commit it. Keep going until a new log entry is not on a majority of servers
	for i := (rf.CommitIndex + 1); i < countsLen; i++ {
		if ((counts[i] + 1) * 2) > len(rf.peers) {
			logDebug(fmt.Sprintf("%s:Log[%d]=%v is replicated on %d/%d machines... committing\n", 
					rf.toString(), i, rf.Logs[i], counts[i] + 1, len(rf.peers)))

			rf.CommitIndex = i
		} else {
			break;
		}
	}
	if oldCommit < rf.CommitIndex { 
		logDebug(fmt.Sprintf("%s:Committed %d new entries:%v ... CommitIndex moving from %d to %d\n", 
			rf.toString(), rf.CommitIndex - oldCommit, rf.Logs[(oldCommit + 1):(rf.CommitIndex + 1)], oldCommit, rf.CommitIndex)) 
		logDebug(fmt.Sprintf("%s:New Commit Log:%v\n", rf.toString(), rf.Logs[:rf.CommitIndex + 1]))
	}

	go rf.persist()

	rf.mu.Unlock()
	if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:commitNewEntries() RELEASES the Lock\n", rf.toString())) }
}

func (rf *Raft) applyState(applyCh chan ApplyMsg){
	for {
		rf.commitNewEntries()
		time.Sleep(time.Duration(APPLY_STATE_TIMEOUT) * time.Millisecond)

		rf.mu.Lock()
		if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:applyState() HAS the Lock\n", rf.toString())) }

		if rf.lastApplied < rf.CommitIndex {
			for i := rf.lastApplied + 1; i <= rf.CommitIndex; i++ {
				applyMsg := ApplyMsg{}
				//Adjust to 1 indexed logs for return
				applyMsg.Index = i + 1
				applyMsg.Command = rf.Logs[i].Command
				applyCh <- applyMsg
			}
			rf.lastApplied = rf.CommitIndex
		}

		rf.mu.Unlock()
		if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:applyState() RELEASES the Lock\n", rf.toString())) }
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:Start() HAS the Lock\n", rf.toString())) }

	var index int = 0
	term := rf.CurrentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		logDebug(fmt.Sprintf("%s:Received command %v from Client\n", 
			rf.toString(), command)) 

		newEntry := LogEntry{}
		newEntry.Command = command
		newEntry.Term = term
		rf.Logs = append(rf.Logs, newEntry)
		//return the value it sits at in the LogEntries array is the index it will be committed at (1 indexed)
		index = len(rf.Logs)
	}

	rf.mu.Unlock()
	if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:Start() RELEASES the Lock\n", rf.toString())) }

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

//Get a random election timeout (between 10ms and 500ms)
func randTimeoutVal(low int, high int) int {
	return rand.Intn(high - low) + low

}

//Run the server
func (rf *Raft) run() {
	for {
		switch rf.state {
		case LEADER:
			heartbeatTimeout := time.After(time.Duration(HEARTBEAT_TIMEOUT) * time.Millisecond)
			select {
			//If this leader discovers it needs to stand down then move to the follower state
			case <-rf.followerCh:
				rf.mu.Lock()
				if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:run() LEADER <-rf.followerCh HAS the Lock\n", rf.toString())) }

				logDebug(fmt.Sprintf("%s:Standing down to Follower\n", rf.toString()))

				rf.state = FOLLOWER
				rf.VotedFor = -1
				rf.VotesFor = 0

				rf.mu.Unlock()
				if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:run() LEADER <-rf.followerCh RELEASES the Lock\n", rf.toString())) }
			//Otherwise broadcast heartbeats
			case <-heartbeatTimeout:
				rf.broadcastAppendEntries()
			}
		case FOLLOWER:
			electionTimeoutVal := randTimeoutVal(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
			electionTimeout := time.After(time.Duration(electionTimeoutVal) * time.Millisecond)
			select {
			//If you get a heartbeat as a follower do nothing
			case <-rf.followerCh:
			//If you timeout then transition to the candidate phase
			case <-electionTimeout:
				rf.mu.Lock()
				logLockDebug(fmt.Sprintf("%s:run() FOLLOWER <-electionTimeout HAS the Lock\n", rf.toString()))

				logDebug(fmt.Sprintf("%s:Timeout moving to Candidate\n", rf.toString()))
				rf.state = CANDIDATE

				rf.mu.Unlock()
				if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:run() FOLLOWER <-electionTimeout RELEASES the Lock\n", rf.toString())) }
			}
		case CANDIDATE:
			//Increment term and vote for yourself
			rf.mu.Lock()
			if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:run() CANDIDATE HAS the Lock\n", rf.toString())) }

			rf.CurrentTerm += 1
			rf.VotedFor = rf.me
			rf.VotesFor += 1

			logDebug(fmt.Sprintf("%s:Starting election for Term:%d\n", rf.toString(), rf.CurrentTerm))

			rf.mu.Unlock()
			if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:run() CANDIDATE RELEASES the Lock\n", rf.toString())) }

			//Start the election
			electionTimeoutVal := randTimeoutVal(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
			electionTimeout := time.After(time.Duration(electionTimeoutVal) * time.Millisecond)
			//Send request vote RPCs to all other servers
			rf.broadcastRequestVote()
			select {
			//If you get a valid heartbeat from a leader then revert to follower
			case <-rf.followerCh:
				rf.mu.Lock()
				if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:run() CANDIDATE <-rf.followerCh HAS the Lock\n", rf.toString())) }

				// logDebug(fmt.Sprintf("%s:Got an AppendEntry or RequestVote of higher term moving to Follower\n", rf.toString()))

				rf.state = FOLLOWER
				rf.VotedFor = -1
				rf.VotesFor = 0

				rf.mu.Unlock()
				if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:run() CANDIDATE <-rf.followerCh RELEASES the Lock\n", rf.toString())) }
			//If you get enough votes to become the leader then transition to the leader state
			case <-rf.leaderCh:
				if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:Trying to grab lock to move to LEADER\n", rf.toString())) }

				rf.mu.Lock()
				if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:run() CANDIDATE <-rf.leaderCh HAS the Lock\n", rf.toString())) }

				logDebug(fmt.Sprintf("%s:Got enough votes moving to Leader\n", rf.toString()))

				rf.state = LEADER
				rf.VotedFor = -1
				rf.VotesFor = 0
				//When a leader comes to power initialize NextIndex to be the 1 greater than the last entry in the new leader's log
				for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
					rf.NextIndex[peerNum] = len(rf.Logs)
				}

				rf.mu.Unlock()
				if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:run() CANDIDATE <-rf.leaderCh RELEASES the Lock\n", rf.toString())) }
			//If you timeout without winning or losing remain a %d and start the election over
			case <-electionTimeout:
				rf.mu.Lock()
				if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:run() CANDIDATE <-electionTimeout HAS the Lock\n", rf.toString())) }

				logDebug(fmt.Sprintf("%s:Election timeout (Term:%d)\n", rf.toString(), rf.CurrentTerm))

				rf.VotedFor = -1
				rf.VotesFor = 0

				rf.mu.Unlock()
				if rf.state != FOLLOWER { logLockDebug(fmt.Sprintf("%s:run() CANDIDATE <-electionTimeout RELEASES the Lock\n", rf.toString())) }
			}
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
	rf.peerNums = make([]int, 0)
	for i := 0; i < len(peers); i++ {
		if i != me { rf.peerNums = append(rf.peerNums, i) }
	}

	rf.state = FOLLOWER
	rf.CurrentTerm = 0
	rf.Logs = make([]LogEntry, 0)
	rf.NextIndex = make([]int, len(peers)) 
	rf.matchIndex = make([]int, len(peers))
	rf.followerCh = make(chan bool)
	rf.candidateCh = make(chan bool)
	rf.leaderCh = make(chan bool)
	rf.VotedFor = -1
	rf.VotesFor = 0
	rf.CommitIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//Run the main server thread
	fmt.Printf("Started up Peer:%d on Term:%d\n", rf.me, rf.CurrentTerm)
	go rf.run()
	//need to send ApplyMsgs on the applyCh
	go rf.applyState(applyCh)

	return rf
}
