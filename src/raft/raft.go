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
)

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
const ELECTION_TIMEOUT_MAX = 300
const HEARTBEAT_TIMEOUT = 20
const APPLY_STATE_TIMEOUT = 30
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
	// Your code here.
	// Example:
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
	if (LOCK_DEBUG && rf.state != FOLLOWER) { fmt.Printf("<Peer:%d Term:%d State:%s>:Trying to grab lock to move to process RequestVote\n", rf.me, rf.CurrentTerm, stateToString(rf.state))}

	rf.mu.Lock()
	if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:RequestVote() HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

	lastLogIndex := len(rf.Logs) - 1
	var lastLogTerm int
	if lastLogIndex > -1 {
		lastLogTerm = rf.Logs[lastLogIndex].Term 
	} else {
		lastLogTerm = 0
	}	

	//If the term for the requested vote is less than the current term 
	if args.Term < rf.CurrentTerm {
		if DEBUG { 
			fmt.Printf("<Peer:%d Term:%d State:%s>:DENIED RequestVote from:<Peer:%d Term:%d> (Term is behind)\n", 
				rf.me, rf.CurrentTerm, stateToString(rf.state), args.CandidateId, args.Term) 
		}
		//Dont grant the vote
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	} else if args.Term == rf.CurrentTerm {
	//If the terms are the same
		//If this peer has already voted or the vote requester's log is behind this peer's log dont grant the vote
		// A vote requester's log is behind if:
		// 1) The lastLogTerm of the vote requester is less than the lastLogTerm of this peer
		// 2) If the lastLogTerms are equal and the lastLogIndex of the vote requester is less than the LastLogIndex of this peer
		if rf.VotedFor > -1 || args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			if DEBUG { 
				fmt.Printf("<Peer:%d Term:%d State:%s>:DENIED RequestVote from:<Peer:%d Term:%d> (Already Voted in this term or their logs are behind)\n", 
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
		//If the vote requester's log is behind this peer's log dont grant the vote
		// A vote requester's log is behind if:
		// 1) The lastLogTerm of the vote requester is less than the lastLogTerm of this peer
		// 2) If the lastLogTerms are equal and the lastLogIndex of the vote requester is less than the LastLogIndex of this peer		
		if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			if DEBUG { 
				fmt.Printf("<Peer:%d Term:%d State:%s>:DENIED RequestVote from:<Peer:%d Term:%d> (Their logs are behind)\n", 
					rf.me, rf.CurrentTerm, stateToString(rf.state), args.CandidateId, args.Term) 
			}
			reply.VoteGranted = false
		} else {
			if DEBUG { 
				fmt.Printf("<Peer:%d Term:%d State:%s>:GRANTED RequestVote from:<Peer:%d Term:%d>\n", 
					rf.me, rf.CurrentTerm, stateToString(rf.state), args.CandidateId, args.Term) 
			}
			rf.VotedFor = args.CandidateId
			reply.VoteGranted = true
		}

		//Update this peers term to the candidate's term
		rf.CurrentTerm = args.Term
		reply.Term = rf.CurrentTerm
		//TODO: this isnt a true heartbeat, might want to just change the state here?
		rf.followerCh <- true
		// rf.state = FOLLOWER
		// rf.VotedFor = -1
		// rf.VotesFor = 0
	}
	rf.mu.Unlock()
	if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:RequestVote() RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
}

func (rf *Raft) broadcastRequestVote(){
	//Iterate through the peers and send a request vote to each
	for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
		rf.mu.Lock()
		if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:broadcastRequestVote() HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

		// if rf.state == LEADER {
		// 	rf.mu.Unlock()
		// 	if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:broadcastRequestVote() RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
		// 	break
		// }

		if peerNum != rf.me {
			//Set up the sendRequestVote args
			args := &RequestVoteArgs{}
			args.Term = rf.CurrentTerm
			args.CandidateId = rf.me
			//TODO
			lastLogIndex := len(rf.Logs) - 1
			args.LastLogIndex = lastLogIndex
			if lastLogIndex < 0 {
				args.LastLogTerm = 0
			} else {
				args.LastLogTerm = rf.Logs[lastLogIndex].Term
			}
			
			
			//Set up the sendRequestVote reply
			reply := &RequestVoteReply{}
			go rf.sendRequestVote(peerNum, *args, reply)
		}
		rf.mu.Unlock()
		if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:broadcastRequestVote() RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:sendRequestVote() HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

		//If you got the vote and 
		if rf.state == CANDIDATE && reply.VoteGranted {
			rf.VotesFor += 1
			//If are still a candidate and you have the majority of the votes in this election
			if (rf.VotesFor * 2) > len(rf.peers) {
				//Let the main thread know that you have enough votes to be leader
				select {
				case rf.leaderCh <- true:
				default:
					if DEBUG { 
						fmt.Printf("<Peer:%d Term:%d State:%s>leaderCh is already full\n", 
							rf.me, rf.CurrentTerm, stateToString(rf.state)) 
					}		
				}
				
			}
		}

		rf.mu.Unlock()
		if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:sendRequestVote() RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
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
	if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:AppendEntries() HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

	//Reply success=false if the leaders term is less than this peers term
	if args.Term < rf.CurrentTerm {
		if DEBUG { 
			fmt.Printf("<Peer:%d Term:%d State:%s>:Got Faulty HEARTBEAT from Leader:<Peer:%d Term:%d>\n", 
				rf.me, rf.CurrentTerm, stateToString(rf.state), args.LeaderId, args.Term) 
		}
		reply.Term = rf.CurrentTerm
		reply.MatchIndex = len(rf.Logs) - 1
		reply.Success = false
	} else {
	//Otherwise reply success=true
		//If this peers term is less than the leaders term update it
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
		//Check that the lastLog entry on this server is the same index and term as the lastLog on the leader
		if args.PrevLogIndex == lastLogIndex && args.PrevLogTerm == lastLogTerm {
			if (DEBUG && len(args.Entries) > 0) { 
				fmt.Printf("<Peer:%d Term:%d State:%s>:Got HEARTBEAT from Leader:<Peer:%d, Term:%d> with MATCHING lastLogIndex (l:%d/f:%d) and lastTerm (l:%d/f:%d)\n", 
					rf.me, rf.CurrentTerm, stateToString(rf.state), args.LeaderId, args.Term, args.PrevLogIndex, lastLogIndex, args.PrevLogTerm, lastLogTerm) 
			}
			//If the leader has more comitted entries than this raft then update this raft
			if rf.CommitIndex < args.LeaderCommit {
				rf.CommitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.Logs) - 1)))
			}

			//Add all new log entries to this server
			for i := 0; i < len(args.Entries); i++ {
				rf.Logs = append(rf.Logs, args.Entries[i]) 
			}

			go rf.persist()

			reply.MatchIndex = len(rf.Logs) - 1
			reply.Success = true
		} else {
			if (DEBUG) { 
				fmt.Printf("<Peer:%d Term:%d State:%s>:Got a HEARTBEAT from Leader:<Peer:%d, Term:%d> with MISMATCHED lastLogIndex (l:%d/f:%d) or lastTerm (l:%d/f:%d)\n", 
					rf.me, rf.CurrentTerm, stateToString(rf.state), args.LeaderId, args.Term, args.PrevLogIndex, lastLogIndex, args.PrevLogTerm, lastLogTerm) 
			}
			//TODO
			//If the this peer's log has more entries than the leader's log we need to delete them
			if lastLogIndex > args.PrevLogIndex {
				origLogLength := len(rf.Logs)
				rf.Logs = rf.Logs[:(args.PrevLogIndex + 1)]

				if (DEBUG) { 
					fmt.Printf("<Peer:%d Term:%d State:%s>:Deleting %d entries from this peers log to match it (l:%d/f:%d) with Leader:<Peer:%d, Term:%d>\n", 
						rf.me, rf.CurrentTerm, stateToString(rf.state), origLogLength - len(rf.Logs), args.PrevLogIndex, len(rf.Logs) - 1, args.LeaderId, args.Term) 
				}

				reply.MatchIndex = len(rf.Logs) - 1
			} else if lastLogIndex == args.PrevLogIndex || lastLogTerm != args.PrevLogTerm {
				if (DEBUG) { 
					fmt.Printf("<Peer:%d Term:%d State:%s>:Log Lengths match Leader:<Peer:%d, Term:%d> but terms don't (l:%d/f:%d). Deleting 1 entry from this peer's log\n", 
						rf.me, rf.CurrentTerm, stateToString(rf.state), args.LeaderId, args.Term, args.PrevLogTerm, lastLogTerm) 
				}
				rf.Logs = rf.Logs[:lastLogIndex]
				reply.MatchIndex = lastLogIndex - 1
			} else {
			//If this peer is behind the leader then just let the leader know
				reply.MatchIndex = len(rf.Logs) - 1
				if (DEBUG) { 
					fmt.Printf("<Peer:%d Term:%d State:%s>:This peer is behind (l:%d/f:%d) Leader:<Peer:%d, Term:%d>\n", 
						rf.me, rf.CurrentTerm, stateToString(rf.state), args.PrevLogIndex, len(rf.Logs) - 1, args.LeaderId, args.Term) 
				}
			}
			reply.Success = false
		}
		
		//Send the heartbeat notice to the main server thread
		rf.followerCh <- true
	}
	rf.mu.Unlock()
	if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:AppendEntries() RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
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

func (rf *Raft) broadcastAppendEntries(){
	//Iterate through all peers and send append entries to each
	for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
		if peerNum != rf.me {
			rf.mu.Lock()
			if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:broadcastAppendEntries() HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

			args := &AppendEntriesArgs{}
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			//Get the index of the next log entry to send to this peer
			nextLogIdx := rf.NextIndex[peerNum]
			//Set the info of the previous log entry (immediately preceeding nextLogIdx)
			args.PrevLogIndex = nextLogIdx - 1
			if args.PrevLogIndex > -1 {
				args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term 
			} else {
				args.PrevLogTerm = 0
			}
			args.Entries = rf.Logs[nextLogIdx:]
			if (DEBUG && len(args.Entries) > 0) { 
				fmt.Printf("<Peer:%d Term:%d State:%s>:Broadcasting %d new Log Entries to peer:%d\n", 
					rf.me, rf.CurrentTerm, stateToString(rf.state), len(args.Entries), peerNum) 
			}
			args.LeaderCommit = rf.CommitIndex

			reply := &AppendEntriesReply{}

			rf.mu.Unlock()
			if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:broadcastAppendEntries() HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

			//Each heartbeat call should be its own thread
			go rf.sendAppendEntries(peerNum, *args, reply)
		}		
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:sendAppendEntries() HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

		if rf.state != LEADER {
			if (DEBUG) { 
				fmt.Printf("<Peer:%d Term:%d State:%s>:Isn't a leader anymore, not fielding heartbeat replies\n", 
					rf.me, rf.CurrentTerm, stateToString(rf.state)) 
			}
			rf.mu.Unlock()
			return ok
		}

		if !reply.Success {
			//If the leaders term is less than the term of the peer the leader needs to step down
			if rf.CurrentTerm < reply.Term {
				//fmt.Printf("Leader found out it is bad\n");
				if (DEBUG) { 
					fmt.Printf("<Peer:%d Term:%d State:%s>:Found out it's behind (Term:%d)\n", 
						rf.me, rf.CurrentTerm, stateToString(rf.state), reply.Term) 
				}
				rf.CurrentTerm = reply.Term
				rf.followerCh <- true
			} else {
			//Otherwise we need to update the MatchIndex and NextIndex NextIndex according to the returned MatchIndex
				if (DEBUG) { 
					fmt.Printf("<Peer:%d Term:%d State:%s>:AppendEntriesRPC to <Peer:%d> returned FALSE, changing MatchIndex to %d and NextIndex to %d\n", 
						rf.me, rf.CurrentTerm, stateToString(rf.state), server, reply.MatchIndex, reply.MatchIndex + 1) 
				}
				rf.matchIndex[server] = reply.MatchIndex
				rf.NextIndex[server] = reply.MatchIndex + 1
				// if rf.NextIndex[server] < 0 { rf.NextIndex[server] = 0 }
			}	
		} else {
			//If the call was successful then update the leaders matchIndex array
			rf.matchIndex[server] = int(math.Max(float64(reply.MatchIndex), float64(rf.matchIndex[server])))
			//Also update the next index array appropriately
			rf.NextIndex[server] = reply.MatchIndex + 1
		}

		rf.mu.Unlock()
		if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:sendAppendEntries() RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

	}
	return ok
}

func (rf *Raft) commitNewEntries(){
	rf.mu.Lock()
	if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:commitNewEntries() HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

	if rf.state != LEADER || len(rf.Logs) == 0 {
		rf.mu.Unlock()
		if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:commitNewEntries() RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) } 
		return 
	}

	// if DEBUG { fmt.Printf("<Peer:%d Term:%d State:%s>:Is trying to commit new entries\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
	//Calculate how many uncomitted log entries the leader has
	//countsLen := len(rf.Logs) - (rf.CommitIndex + 1)
	countsLen := len(rf.Logs)
	//Create an array that holds the count of number of servers a LogEntry is replicated on
	counts := make([]int, countsLen)

	//Iterate through the leaders matchIndex array and see how many peers have newer log entries replicated on them
	for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
		//If this peer has a later log on it than the last commit
		if peerNum != rf.me && rf.matchIndex[peerNum] > rf.CommitIndex {
			for countIdx := rf.CommitIndex + 1; countIdx < rf.matchIndex[peerNum] + 1; countIdx++ {
				counts[countIdx] += 1
			}
			//counts[rf.matchIndex[peerNum]] += 1
		}
	}
	oldCommit := rf.CommitIndex
	//If a majority of servers have a certain new LogEntry, commit it. Keep going until a new log entry is not on a majority of servers
	for i := (rf.CommitIndex + 1); i < countsLen; i++ {
		//If a majority of peers have this Log entry applied then increase the commit index to this log entry
		if ((counts[i] + 1) * 2) > len(rf.peers) {
			if (DEBUG) { 
				fmt.Printf("LogEntry %d is replicated on %d machines\n", 
					i, counts[i] + 1) 
			}		
			rf.CommitIndex = i
		} else {
			break;
		}
	}
	if (DEBUG && oldCommit < rf.CommitIndex) { 
		fmt.Printf("<Peer:%d Term:%d State:%s>:CommitIndex moving from %d to %d\n", 
			rf.me, rf.CurrentTerm, stateToString(rf.state), oldCommit, rf.CommitIndex) 
	}

	go rf.persist()

	rf.mu.Unlock()
	if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:commitNewEntries() RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

}

func (rf *Raft) applyState(applyCh chan ApplyMsg){
	for {
		rf.commitNewEntries()
		time.Sleep(time.Duration(APPLY_STATE_TIMEOUT) * time.Millisecond)

		rf.mu.Lock()
		if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:applyState() HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

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
		if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:applyState() RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

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
	if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:Start() HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }


	var index int = 0
	term := rf.CurrentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		newEntry := LogEntry{}
		newEntry.Command = command
		newEntry.Term = term
		if (DEBUG) { 
			fmt.Printf("<Peer:%d Term:%d State:%s>:Received command %d from client\n", 
				rf.me, rf.CurrentTerm, stateToString(rf.state), command) 
		}
		rf.Logs = append(rf.Logs, newEntry)
		//return the value it sits at in the LogEntries array is the index it will be committed at (1 indexed)
		index = len(rf.Logs)
	}

	rf.mu.Unlock()
	if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:Start() RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

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
				if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() LEADER <-rf.followerCh HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:%s>:Standing down to FOLLOWER\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
				rf.state = FOLLOWER
				rf.VotedFor = -1
				rf.VotesFor = 0

				rf.mu.Unlock()
				if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() LEADER <-rf.followerCh RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
			//Otherwise broadcast heartbeats
			case <-heartbeatTimeout:
				// rf.mu.Lock()
				// if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() LEADER <-heartbeatTimeout HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

				// if DEBUG { fmt.Printf("<Peer:%d Term:%d State:%s>: Sending HEARTBEATS\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
				
				// rf.mu.Unlock()
				// if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() LEADER <-heartbeatTimeout RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

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
				if LOCK_DEBUG { fmt.Printf("<Peer:%d Term:%d State:%s>:run() FOLLOWER <-electionTimeout HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:%s>:Timeout moving to CANDIDATE\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
				rf.state = CANDIDATE

				rf.mu.Unlock()
				if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() FOLLOWER <-electionTimeout RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
			}
		case CANDIDATE:
			//Increment term and vote for yourself
			rf.mu.Lock()
			if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() CANDIDATE HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

			rf.CurrentTerm += 1
			rf.VotedFor = rf.me
			rf.VotesFor += 1

			rf.mu.Unlock()
			if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() CANDIDATE RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
			//Start the election
			electionTimeoutVal := randTimeoutVal(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
			electionTimeout := time.After(time.Duration(electionTimeoutVal) * time.Millisecond)
			//Send request vote RPCs to all other servers
			rf.broadcastRequestVote()
			select {
			//If you get a valid heartbeat from a leader then revert to follower
			case <-rf.followerCh:
				rf.mu.Lock()
				if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() CANDIDATE <-rf.followerCh HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:%s>:Got HEARTBEAT moving to FOLLOWER\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
				rf.state = FOLLOWER
				rf.VotedFor = -1
				rf.VotesFor = 0

				rf.mu.Unlock()
				if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() CANDIDATE <-rf.followerCh RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
			//If you get enough votes to become the leader then transition to the leader state
			case <-rf.leaderCh:
				if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:Trying to grab lock to move to LEADER\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

				rf.mu.Lock()
				if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() CANDIDATE <-rf.leaderCh HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:%s>:Got enough votes moving to LEADER\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
				rf.state = LEADER
				rf.VotedFor = -1
				rf.VotesFor = 0
				//When a leader comes to power initialize NextIndex to be the 1 greater than the last entry in the new leader's log
				for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
					rf.NextIndex[peerNum] = len(rf.Logs)
				}

				rf.mu.Unlock()
				if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() CANDIDATE <-rf.leaderCh RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
			//If you timeout without winning or losing remain a %d and start the election over
			case <-electionTimeout:
				rf.mu.Lock()
				if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() CANDIDATE <-electionTimeout HAS the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }

				if DEBUG { fmt.Printf("<Peer:%d Term:%d State:%s>:Election timeout, starting new election (Term:%d)\n", rf.me, rf.CurrentTerm, stateToString(rf.state), rf.CurrentTerm + 1) }
				rf.VotedFor = -1
				rf.VotesFor = 0

				rf.mu.Unlock()
				if LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("<Peer:%d Term:%d State:%s>:run() CANDIDATE <-electionTimeout RELEASES the Lock\n", rf.me, rf.CurrentTerm, stateToString(rf.state)) }
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
