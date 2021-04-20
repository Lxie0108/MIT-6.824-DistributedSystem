package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {//send to RAFT
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Type string //such as Put/Append
	ClientId int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg //send to RAFT by this applyCh
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db                   map[string]string
	mapCh				 map[int] chan Op
	mapRequest 			 map[int64]int //clientId to requestId
	persist  			 *raft.Persister

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) { //rpc handler
	// Your code here.
	op1 := Op {
        Type: "Get", 
        Key: args.Key,
        Value: "",
    }
	reply.IsLeader = false
    index,_,isLeader := kv.rf.Start(op1)
    if !isLeader{
        return
    }
    channel := kv.putIfAbsent(index)
    op2 := kv.waitCommitting(channel)
    if op1.Key == op2.Key && op1.Value == op2.Value && op1.Type == op2.Type && op1.ClientId == op2.ClientId && op1.RequestId == op2.RequestId {//if not equal, it indicates that a the client's operation has failed
		reply.IsLeader = true
		kv.mu.Lock()
        reply.Value = kv.db[op2.Key]
		kv.mu.Unlock()
        return
    }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) { //rpc handler
	// Your code here. 
	op1 := Op {
        Type: args.Op, 
        Key: args.Key,
        Value: args.Value,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
    }
	reply.IsLeader = false
    index,_,isLeader := kv.rf.Start(op1)
    if !isLeader{
         return
    }
    channel := kv.putIfAbsent(index)
    op2 := kv.waitCommitting(channel)
    if op1.Key == op2.Key && op1.Value == op2.Value && op1.Type == op2.Type && op1.ClientId == op2.ClientId && op1.RequestId == op2.RequestId {
		reply.IsLeader = true
         return
    }
}

func (kv *KVServer) putIfAbsent(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.mapCh[index]; !ok {
		kv.mapCh[index] = make(chan Op, 1)
	}
	return kv.mapCh[index]
}

//added timeout logic so that client does not get blocked when raft leader can't commit/
func (kv *KVServer) waitCommitting(channel chan Op) Op {
	select {
	case op2 := <- channel:
		return op2
	case <- time.After(500*time.Millisecond):
		return Op{}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

/** read snapshot to restore previous persistd states.
**/
func (kv *KVServer) readSnapshot(snapshot []byte){
	if snapshot == nil {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var mapRequest map[int64]int
	if d.Decode(&db) != nil || d. Decode(&mapRequest) != nil {
	   //error...
	   DPrintf("%v fails to read Snapshot", kv.me)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		kv.db = db
		kv.mapRequest = mapRequest
	}
}

func (kv *KVServer) requireTrimming() bool{
	if kv.maxraftstate == -1 {
		return false
	}
	if kv.rf.GetRaftStateSize() >= kv.maxraftstate {
		return true
	}
	return false
}

func (kv *KVServer) snapshot(){

}



//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persist = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.mapCh = make(map[int] chan Op)
	kv.mapRequest = make(map[int64]int) 
	kv.readSnapshot(kv.persist.ReadSnapshot())//restore the snapshot from persister

	// You may need initialization code here.
	// This goroutine reads the ApplyMsg from applyCh, and execute. After execution, it notifies the op handler.
	go func() {
		for applyMsg := range kv.applyCh {
			if applyMsg.CommandValid == false {
				continue
			}
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			idRequest, ok := kv.mapRequest[op.ClientId]
			if !ok || op.RequestId > idRequest {
				switch op.Type {
				case "Put":
					kv.db[op.Key] = op.Value
				case "Append":
					kv.db[op.Key] += op.Value
				}
				kv.mapRequest[op.ClientId] = op.RequestId
			}
			kv.mu.Unlock()
			index := applyMsg.CommandIndex
			channel := kv.putIfAbsent(index)
			channel <- op
		}
	}()
	
	return kv
}
