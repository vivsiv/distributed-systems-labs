package raftkv

import (
	"labrpc"
	"crypto/rand"
	"math/big"
	"fmt"
)




type Clerk struct {
	servers    []*labrpc.ClientEnd
	me         int64 //this clerks id
	lastLeader int
	DEBUG      bool
	// You will have to modify this struct.
}

func (ck *Clerk) toString() string {
	return fmt.Sprintf("<Client:%d>", ck.me)
}

func (ck *Clerk) logDebug(msg string){
	if ck.DEBUG { fmt.Printf("%s:%s\n", ck.toString(), msg) }
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.me = nrand()
	ck.lastLeader = 0
	ck.DEBUG = true

	ck.logDebug(fmt.Sprintf("Started Up"))

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	//Want to iterate through servers starting at the lastLeader
	servNum := ck.lastLeader
	//Keep trying request until it succeeds
	for {
		reply := GetReply{}

		//ck.logDebug(fmt.Sprintf("Sending Get RPC:%v to server %d", args, servNum))

		ok := ck.servers[servNum].Call("RaftKV.Get", args, &reply)

		if ok {
			wrongLeader := reply.WrongLeader
			errCode := reply.Err
			value := reply.Value
			if wrongLeader {
				//ck.logDebug(fmt.Sprintf("Server %d is no longer a leader", servNum))
			} else if errCode != OK {
				ck.lastLeader = servNum
				ck.logDebug(fmt.Sprintf("Got ErrorCode %s", errCode))
				return ""
			} else {
				ck.lastLeader = servNum
				ck.logDebug(fmt.Sprintf("Got Value %s", value))
				return value
			}
			
		} else {
			ck.logDebug(fmt.Sprintf("RPC call to server %d failed", servNum))
		}

		servNum = (servNum + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, opType OpType) {
	args := PutAppendArgs{}
	args.Type = opType
	args.Key = key
	args.Value = value

	//Want to iterate through servers starting at the lastLeader
	servNum := ck.lastLeader
	//Keep trying request until it succeeds
	for {
		reply := PutAppendReply{}

		//ck.logDebug(fmt.Sprintf("Sending %s RPC:%v to server %d", opType, args, servNum))
		ok := ck.servers[servNum].Call("RaftKV.PutAppend", args, &reply)

		if ok {
			wrongLeader := reply.WrongLeader
			errCode := reply.Err
			if wrongLeader {
				//ck.logDebug(fmt.Sprintf("Server %d is no longer a leader", servNum))

			} else if errCode != OK {
				ck.logDebug(fmt.Sprintf("Got ErrorCode %s", errCode))

				ck.lastLeader = servNum
				return
			} else {
				ck.logDebug(fmt.Sprintf("RPC %v succeeded", args))

				ck.lastLeader = servNum
				return
			}
		} else {
			ck.logDebug(fmt.Sprintf("RPC call to server %d failed", servNum))
		}

		servNum = (servNum + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.logDebug(fmt.Sprintf("Put(key:%s, value:%s) called", key, value))
	ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.logDebug(fmt.Sprintf("Append(key:%s, value:%s) called", key, value))
	ck.PutAppend(key, value, Append)
}
