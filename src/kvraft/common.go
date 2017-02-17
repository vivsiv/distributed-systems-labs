package raftkv

// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug > 0 {
// 		log.Printf(format, a...)
// 	}
// 	return
// }

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	Timeout  = "Timeout"
)

type Err string

type OpType string

const (
	Get = "Get"
	Put = "Put"
	Append = "Append"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key    string
	Value  string
	Type    OpType // "Put" or "Append"
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
