package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "math"
import "time"


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
	Type string //such as Join/Leave
	ClientId int64
	RequestId int
	Args interface{} // such as JoinArgs/LeaveArgs
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op1 := Op {
        Type: "Join", 
        ClientId: args.ClientId,
        RequestId: args.RequestId,
		Args: *args,
    }
	reply.WrongLeader = true
    index,_,isLeader := sc.rf.Start(op1)
    if !isLeader{
        return
    }
    channel := sc.putIfAbsent(index)
    op2 := sc.waitCommitting(channel)
    if op1.Type == op2.Type && op1.ClientId == op2.ClientId && op1.RequestId == op2.RequestId {//if not equal, it indicates that a the client's operation has failed
		reply.WrongLeader = false
        return
    }
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op1 := Op {
        Type: "Leave", 
        ClientId: args.ClientId,
        RequestId: args.RequestId,
		Args: *args,
    }
	reply.WrongLeader = true
    index,_,isLeader := sc.rf.Start(op1)
    if !isLeader{
        return
    }
    channel := sc.putIfAbsent(index)
    op2 := sc.waitCommitting(channel)
    if op1.Type == op2.Type && op1.ClientId == op2.ClientId && op1.RequestId == op2.RequestId {//if not equal, it indicates that a the client's operation has failed
		reply.WrongLeader = false
        return
    }
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op1 := Op {
        Type: "Move", 
        ClientId: args.ClientId,
        RequestId: args.RequestId,
		Args: *args,
    }
	reply.WrongLeader = true
    index,_,isLeader := sc.rf.Start(op1)
    if !isLeader{
        return
    }
    channel := sc.putIfAbsent(index)
    op2 := sc.waitCommitting(channel)
    if op1.Type == op2.Type && op1.ClientId == op2.ClientId && op1.RequestId == op2.RequestId {//if not equal, it indicates that a the client's operation has failed
		reply.WrongLeader = false
        return
    }
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op1 := Op {
        Type: "Query", 
        ClientId: args.ClientId,
        RequestId: -1,
		Args: *args,
    }
	reply.WrongLeader = true
    index,_,isLeader := sc.rf.Start(op1)
    if !isLeader{
        return
    }
    channel := sc.putIfAbsent(index)
    op2 := sc.waitCommitting(channel)
    if op1.Type == op2.Type && op1.ClientId == op2.ClientId && op1.RequestId == op2.RequestId {//if not equal, it indicates that a the client's operation has failed
		reply.WrongLeader = false
        return
    }
	if !reply.WrongLeader {
		if args.Num < 0 || args.Num >=  len(sc.configs) {
            reply.Config = sc.configs[len(sc.configs) - 1]
        } else {
            reply.Config = sc.configs[args.Num]
        }
	}
}

//added timeout logic so that client does not get blocked when raft leader can't commit/
func (sc *ShardCtrler) waitCommitting(channel chan Op) Op {
	select {
	case op2 := <- channel:
		return op2
	case <- time.After(500*time.Millisecond):
		return Op{}
	}
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

// needed by shardsc tester
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

//return a copy of the current config.
func (sc *ShardCtrler) getConfig() Config{
	return sc.configs[len(sc.configs) - 1]
}

//Do reconfiguration by adjusting shards after each op.Type.
func (sc *ShardCtrler) reconfig(config *Config, opType string, changedGid int){
	mapGidShard := map[int][]int{} // gid -> number of shard
	for gid,_ := range config.Groups {
        mapGidShard[gid] = []int{}
    }
    for shard, gid := range config.Shards {
        mapGidShard[gid] = append(mapGidShard[gid], shard)
    }

	switch opType {
	case "Move":
		//
	case "Join":
		//the new configuration should divide the shards as evenly as possible among the groups, and should move as few shards as possible to achieve that goal.
		average := NShards / len(config.Groups) //num of shards that should be moved to new group
		for i := 0; i < average; i++ {
			max := - math.MaxInt32
			targetGid := -100
        	for gid, shard := range mapGidShard{ // find gid that has the largest number of shards
			   if max <= len(shard) {
				   max = len(shard)
				   targetGid = gid
			   }
			}
			config.Shards[mapGidShard[targetGid][0]] = changedGid //shard -> new joined gid
            mapGidShard[targetGid] = mapGidShard[targetGid][1:] 
        }
	case "Leave":
		//similar to Join() 
		shardsToRemove, exists := mapGidShard[changedGid]
		if !exists {
			return
		}
		delete(mapGidShard, changedGid)
		if len(config.Groups) == 0 { //removed all
			config.Shards = [NShards]int{}
		} else {
			for  _, shard := range shardsToRemove {
				min := math.MaxInt32
				targetGid := -100
				for gid, shard := range mapGidShard{ // find gid that has the largest number of shards
					if min >= len(shard) {
						min = len(shard)
						targetGid = gid
					}
				}
				config.Shards[shard] = changedGid //shard -> new joined gid
				mapGidShard[targetGid] = append(mapGidShard[targetGid],shard)
			}
		}
	}
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
			idRequest, ok := sc.mapRequest[op.ClientId]
			if !ok || op.RequestId > idRequest {
				switch op.Type {
				case "Join": //new GID -> servers mappings
					args := op.Args.(JoinArgs)
					config := sc.getConfig()
					for gid, server := range args.Servers{
						config.Groups[gid] = server
						sc.reconfig(&config,"Join",gid) 
					}
				case "Leave": // GID leaving and new config assigns those groups' shards to the remaining groups
					args := op.Args.(LeaveArgs)
					config := sc.getConfig()
					for _,gid := range args.GIDs{
						delete(config.Groups,gid)
						sc.reconfig(&config,"Leave",gid)
					}
				case "Move": //assign shard to gid
					args := op.Args.(MoveArgs)
					config := sc.getConfig()
					if _,exists := config.Groups[args.GID]; exists {
						config.Shards[args.Shard] = args.GID
					} else {
						return
					}
				case "Query":
					//
				}
				sc.mapRequest[op.ClientId] = op.RequestId
			}
			sc.mu.Unlock()
			index := applyMsg.CommandIndex
			channel := sc.putIfAbsent(index)
			channel <- op
		}
	}()
	return sc
}
