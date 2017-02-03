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
)

// import "bytes"
// import "encoding/gob"

type State int
const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

func stateToString(st State) string {
	switch st {
	case LEADER: 
		return "LEADER"
	case FOLLOWER: 
		return "FOLLOWER"
	case CANDIDATE: 
		return "CANDIDATE"
	default:
		return ""
	}
}

const ELECTION_TIMEOUT_MIN = 150
const ELECTION_TIMEOUT_MAX = 500
const HEARTBEAT_TIMEOUT = 20
const DEBUG = true

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
	peers       []*labrpc.ClientEnd
	persister   *Persister
	me          int // index into peers[]
	state       State //if this Raft thinks its the leader
	CurrentTerm int //last term this raft has seen (starts at 0)
	VotedFor    int //peer that this raft voted for in last term
	Logs        []LogEntry //log entries
	CommitIndex int //index of highest log entry known to be commited
	lastApplied int //index of highest applied log entry
	VotesFor    int
	//Leaders only
    NextIdx     []int //for each server, index of next log entry to send to that server
	matchIdx    []int //for each server, index of highest log entry known to be replicated on server
	followerCh  chan bool
	candidateCh chan bool
	leaderCh    chan bool

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
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
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
	defer rf.mu.Unlock()
	//If the term for the requested vote is less than the current term 
	if args.Term < rf.CurrentTerm {
		if DEBUG { 
			fmt.Printf("<Peer:%d Term:%d State:%s>:DENIED RequestVote from:<Peer:%d Term:%d>\n", 
				rf.me, rf.CurrentTerm, stateToString(rf.state), args.CandidateId, args.Term) 
		}
		//Dont grant the vote
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	} else if args.Term == rf.CurrentTerm {
	//If the terms are the same
		//If this peer has already voted dont grant the vote
		if rf.VotedFor > -1 {
			if DEBUG { 
				fmt.Printf("<Peer:%d Term:%d State:%s>:DENIED RequestVote from:<Peer:%d Term:%d>\n", 
					rf.me, rf.CurrentTerm, stateToString(rf.state), args.CandidateId, args.Term) 
			}
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false 
		} else {
		//If this peer hasn't already voted grant it
			if DEBUG { 
				fmt.Printf("<Peer:%d Term:%d State:%s>:GRANTED RequestVote from:<Peer:%d Term:%d>\n", 
					rf.me, rf.CurrentTerm, stateToString(rf.state), args.CandidateId, args.Term) 
			}
			rf.VotedFor = args.CandidateId
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = true
		}
	} else {
	//If the term for the requested vote is greater than the current term
		if DEBUG { 
			fmt.Printf("<Peer:%d Term:%d State:%s>:GRANTED RequestVote from:<Peer:%d Term:%d>\n", 
				rf.me, rf.CurrentTerm, stateToString(rf.state), args.CandidateId, args.Term) 
		}
		//Update this peers term to the candidate's term
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateId
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		
		//TODO: this isnt a true heartbeat, might want to just change the state here?
		rf.followerCh <- true
		// rf.state = FOLLOWER
		// rf.VotedFor = -1
		// rf.VotesFor = 0
	}
	return
}

func (rf *Raft) broadcastRequestVote(){
	//Iterate through the peers and send a request vote to each
	for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
		rf.mu.Lock()
		if peerNum != rf.me {
			//Set up the sendRequestVote args
			args := &RequestVoteArgs{}
			args.Term = rf.CurrentTerm
			args.CandidateId = rf.me
			//TODO
			args.LastLogIndex = 0
			args.LastLogTerm = rf.CurrentTerm
			
			//Set up the sendRequestVote reply
			reply := &RequestVoteReply{}
			go rf.sendRequestVote(peerNum, *args, reply)
		}
		rf.mu.Unlock()
	}
	


}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		//If you got the vote and 
		if reply.VoteGranted {
			rf.VotesFor += 1
			//If are still a candidate and you have the majority of the votes in this election
			if rf.state == CANDIDATE && (rf.VotesFor * 2) > len(rf.peers) {
				//Let the main thread know that you have enough votes to be leader
				rf.leaderCh <- true
			}
		}
		rf.mu.Unlock()
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
	Term    int //cuurent term 
	Success bool 
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Reply success=false if the leaders term is less than this peers term
	if args.Term < rf.CurrentTerm {
		//fmt.Printf("Got bad heartbeat\n");
		if DEBUG { 
			fmt.Printf("<Peer:%d Term:%d State:%s>:Got Faulty HEARTBEAT from previous term leader:<Peer:%d Term:%d>\n", 
				rf.me, rf.CurrentTerm, stateToString(rf.state), args.LeaderId, args.Term) 
		}
		reply.Term = rf.CurrentTerm
		reply.Success = false
	} else {
	//Otherwise reply success=true
		//If this peers term is less than the leaders term update it
		if rf.CurrentTerm < args.Term {
			rf.CurrentTerm = args.Term
		}
		reply.Term = rf.CurrentTerm
		reply.Success = true
		//Send the heartbeat notice to the main server thread
		rf.followerCh <- true
	}
}

func (rf *Raft) sendHeartbeats(){
	//Iterate through all peers and send heartbeats to each
	for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
		rf.mu.Lock()
		//Dont send heartbeats to yourself
		if peerNum != rf.me {
			args := &AppendEntriesArgs{}
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			//TODO
			args.PrevLogIndex = 0
			args.PrevLogTerm = rf.CurrentTerm
			args.Entries = make([]LogEntry, 1)
			args.LeaderCommit = rf.CommitIndex

			reply := &AppendEntriesReply{}

			//Each heartbeat call should be its own thread
			go rf.sendAppendEntries(peerNum, *args, reply)

		}		
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if !reply.Success {
			rf.mu.Lock()
			//If the leaders term is less than the term of the peer the leader needs to step down
			if rf.CurrentTerm < reply.Term {
				//fmt.Printf("Leader found out it is bad\n");
				if DEBUG { 
					fmt.Printf("<Peer:%d Term:%d State:%s>:Found out it's behind (Term:%d)\n", 
						rf.me, rf.CurrentTerm, stateToString(rf.state), reply.Term) 
				}
				rf.CurrentTerm = reply.Term
				rf.followerCh <- true
			}
			rf.mu.Unlock()
		}
	}
	return ok
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
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	index := rf.CommitIndex
	term := rf.CurrentTerm
	isLeader := rf.state == LEADER

	// if isLeader {
	// 	newEntry := LogEntry{}
	// 	newEntry.Command = command
	// 	newEntry.Term = rf.CurrentTerm
	// 	rf.Logs = append(rf.Logs, newEntry)


	// }

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
				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:LEADER>:Standing down to FOLLOWER\n", rf.me, rf.CurrentTerm) }
				rf.state = FOLLOWER
				rf.VotedFor = -1
				rf.VotesFor = 0
				rf.mu.Unlock()
			//Otherwise send the heartbeat
			case <-heartbeatTimeout:
				rf.mu.Lock()
				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:LEADER>: Sending HEARTBEATS\n", rf.me, rf.CurrentTerm) }
				rf.mu.Unlock()
				rf.sendHeartbeats()
			}
		case FOLLOWER:
			// if DEBUG { fmt.Printf("Peer %d is a FOLLOWER\n", rf.me) }
			electionTimeoutVal := randTimeoutVal(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
			electionTimeout := time.After(time.Duration(electionTimeoutVal) * time.Millisecond)
			select {
			//If you get a heartbeat as a follower do nothing
			case <-rf.followerCh:
				rf.mu.Lock()
				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:FOLLOWER>:Got a HEARTBEAT\n", rf.me, rf.CurrentTerm) }
				rf.mu.Unlock()
			//If you timeout then transition to the candidate phase
			case <-electionTimeout:
				rf.mu.Lock()
				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:FOLLOWER>:Timeout moving to CANDIDATE\n", rf.me, rf.CurrentTerm) }
				rf.state = CANDIDATE
				rf.mu.Unlock()
			}
		case CANDIDATE:
			// if DEBUG { fmt.Printf("Peer %d is a CANDIDATE\n", rf.me) }
			//Increment term and vote for yourself
			rf.mu.Lock()
			rf.CurrentTerm += 1
			rf.VotedFor = rf.me
			rf.VotesFor += 1
			rf.mu.Unlock()
			//Start the election
			electionTimeoutVal := randTimeoutVal(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
			electionTimeout := time.After(time.Duration(electionTimeoutVal) * time.Millisecond)
			//Send request vote RPCs to all other servers
			rf.broadcastRequestVote()
			select {
			//If you get a valid heartbeat from a leader then revert to follower
			case <-rf.followerCh:
				rf.mu.Lock()
				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:CANDIDATE>:Got HEARTBEAT moving to FOLLOWER\n", rf.me, rf.CurrentTerm) }
				rf.state = FOLLOWER
				rf.VotedFor = -1
				rf.VotesFor = 0
				rf.mu.Unlock()
			//If you get enough votes to become the leader then transition to the leader state
			case <-rf.leaderCh:
				rf.mu.Lock()
				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:CANDIDATE>:Got enough votes moving to LEADER\n", rf.me, rf.CurrentTerm) }
				rf.state = LEADER
				rf.VotedFor = -1
				rf.VotesFor = 0
				rf.mu.Unlock()
			//If you timeout without winning or losing remain a candidate and start the election over
			case <-electionTimeout:
				rf.mu.Lock()
				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:CANDIDATE>:Election timeout, starting new election (Term:%d)\n", rf.me, rf.CurrentTerm, rf.CurrentTerm + 1) }
				rf.VotedFor = -1
				rf.VotesFor = 0
				rf.mu.Unlock()
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
	rf.state = FOLLOWER
	rf.CurrentTerm = 0
	rf.Logs = make([]LogEntry, 10)
	rf.NextIdx = make([]int, len(peers)) 
	rf.matchIdx = make([]int, len(peers))
	rf.followerCh = make(chan bool)
	rf.candidateCh = make(chan bool)
	rf.leaderCh = make(chan bool)
	rf.VotedFor = -1
	rf.VotesFor = 0

	// Your initialization code here.
	//Run the main server thread
	fmt.Printf("Started up Peer:%d on Term:%d\n", rf.me, rf.CurrentTerm)
	go rf.run()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//need to send ApplyMsgs on the applyCh

	return rf
}
