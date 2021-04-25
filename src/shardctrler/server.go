package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	mapCh				 map[int] chan Op
	mapRequest 			 map[int64]int //clientId to requestId
}


type Op struct {
	// Your data here.
	Type string //such as Put/Append
	ClientId int64
	RequestId int
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) putIfAbsent(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, ok := sc.mapCh[index]; !ok {
		sc.mapCh[index] = make(chan Op, 1)
	}
	return sc.mapCh[index]
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.mapCh = make(map[int] chan Op)
	sc.mapRequest = make(map[int64]int) 

	go func() {
		for applyMsg := range sc.applyCh {
			if applyMsg.CommandValid == false {
				continue
			}
			op := applyMsg.Command.(Op)
			sc.mu.Lock()
			idRequest, ok := kv.mapRequest[op.ClientId]
			if !ok || op.RequestId > idRequest {
				switch op.Type {
				case "Join":
					//kv.db[op.Key] = op.Value
				case "Leave":
					//kv.db[op.Key] += op.Value
				case "Move":
					//
				case "Query":
					//
				}
				kv.mapRequest[op.ClientId] = op.RequestId
			}
			sc.mu.Unlock()
			index := applyMsg.CommandIndex
			channel := kv.putIfAbsent(index)
			channel <- op
		}
	}()
	return sc
}
