package raftkv

import (
	"encoding/gob"
	"labrpc"
	//"log"
	"raft"
	"sync"
	// "time"
	"fmt"
)

type Op struct {
	Type      OpType
	Key       string
	Value     string
	//ClientId  int 
	//RequestId int //Per client for duplicate detection
}

type PendingOp struct {
	op        *Op
	index     int
	appliedCh chan bool
}

type RaftKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	database     map[string]string
	pendingOps   map[int]*PendingOp
	lastApplied  map[int]int //tracks the last op applied by this server per client
	DEBUG        bool
	LOCK_DEBUG   bool
}

func (kv *RaftKV) toString() string {
	return fmt.Sprintf("<Peer:%d>", kv.me)
}

func (kv *RaftKV) logDebug(msg string) {
	if kv.DEBUG { fmt.Printf("%s:%s\n", kv.toString(), msg) }
}

func (kv *RaftKV) Get(args GetArgs, reply *GetReply) {
	newOp := Op{}
	newOp.Type = Get
	newOp.Key = args.Key

	//Need to Persist Command to Raft
	index, _, isLeader := kv.rf.Start(newOp)

	//If this raft is not the leader just return
	if !isLeader {
		reply.WrongLeader = true 
		return 
	}

	kv.logDebug(fmt.Sprintf("Got Get RPC:%v, persisting to Raft...", newOp))

	newPendingOp := &PendingOp{}
	newPendingOp.op = &newOp
	newPendingOp.index = index
	newPendingOp.appliedCh = make(chan bool)

	kv.mu.Lock()
	kv.pendingOps[index] = newPendingOp
	kv.mu.Unlock()


	//Wait to see if the command gets applied to the raft
	select {
	//This Op gets applied to the raft
	case <-newPendingOp.appliedCh:		
		kv.logDebug(fmt.Sprintf("Notified %v Op was persisted to Raft, updating database...", newOp))
		//Then we can query the database
		kv.mu.Lock()
		value, ok := kv.database[newOp.Key]
		kv.mu.Unlock()

		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
	}

	kv.logDebug(fmt.Sprintf("Returning Reply %v for Get RPC %v", *reply, newOp))
}

func (kv *RaftKV) PutAppend(args PutAppendArgs, reply *PutAppendReply) {
	newOp := Op{}
	newOp.Type = args.Type
	newOp.Key = args.Key
	newOp.Value = args.Value

	//Need to Persist Command to Raft
	index, _, isLeader := kv.rf.Start(newOp)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	newPendingOp := &PendingOp{}
	newPendingOp.op = &newOp
	newPendingOp.index = index
	newPendingOp.appliedCh = make(chan bool)

	kv.mu.Lock()
	kv.pendingOps[index] = newPendingOp
	kv.mu.Unlock()

	kv.logDebug(fmt.Sprintf("Got Put/Append RPC:%v, persisting to Raft...", newOp))

	select {
	//This Op gets applied to the raft
	case <-newPendingOp.appliedCh:
		reply.WrongLeader = false
		reply.Err = OK

		kv.logDebug(fmt.Sprintf("Returning Reply %v for Put/Append RPC %v", *reply, newOp))
	}	
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.DEBUG = false
	kv.rf.Kill()
	// Your code here, if desired.
}


func (kv *RaftKV) run(){
	for {
		//Read apply messages off the apply channel and process them
		appliedMsg := <-kv.applyCh

		appliedIndex := appliedMsg.Index
		//Type inference to case back from interface{}
		appliedOp, ok := appliedMsg.Command.(Op)
		if !ok { continue }

		kv.logDebug(fmt.Sprintf("Applied Op %v to Raft", appliedOp))
		
		kv.mu.Lock()

		//Update this kv's database accordingly
		switch appliedOp.Type {
		case Put:
			kv.database[appliedOp.Key] = appliedOp.Value
			kv.logDebug(fmt.Sprintf("New database %v", kv.database))
		case Append:
			_, ok := kv.database[appliedOp.Key]
			if ok {
				kv.database[appliedOp.Key] += appliedOp.Value
			} else {
				kv.database[appliedOp.Key] = appliedOp.Value
			}

			kv.logDebug(fmt.Sprintf("New database %v", kv.database))
		}


		//Notify any pending ops theyre all done
		pendingOp, ok := kv.pendingOps[appliedIndex]

		if !ok { 
			kv.mu.Unlock()
			continue 
		}

		delete(kv.pendingOps, appliedIndex)
		kv.mu.Unlock()

		//Notify the Op that it has been applied to Raft
		pendingOp.appliedCh <- true
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.database = make(map[string]string)
	kv.pendingOps = make(map[int]*PendingOp)
	kv.lastApplied = make(map[int]int)
	kv.DEBUG = true

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//Start up this kv server
	go kv.run()

	return kv
}
