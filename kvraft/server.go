package raftkv

import (
	"time"
	"encoding/gob"
	"kvdb/labrpc"
	"kvdb/labgob"
	"bytes"
	"log"
	"kvdb/raft"
	"sync"
)

const Debug = 0

const AwaitLeaderCheckInterval = 10 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method string
	Key string
	Value string
	ClientId int64
	SerialNum int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	snapshotsEnabled bool

	isDecommissioned bool

	// Your definitions here.
	isLeader bool

	Kvmap map[string]string

	ClientIdBySerialNum map[int64]int64

	requestHandlers map[int]chan raft.ApplyMsg
}

func (rf *RaftKV) Lock() {
	rf.mu.Lock()
}

func (rf *RaftKV) UnLock() {
	rf.mu.Unlock()
}

func (kv *RaftKV) createSnapshot(logIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Kvmap)
	e.Encode(kv.ClientIdBySerialNum)
	data := w.Bytes()
	kv.rf.SaveSnapShot(data)

	// Compact raft log til index.
	kv.rf.CompactLog(logIndex)
}

func (kv *RaftKV) loadSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	kvmap := make(map[string]string)
	clientIdBySerialNum := make(map[int64]int64)
	d.Decode(&kvmap)
	d.Decode(&clientIdBySerialNum)

	kv.Kvmap = kvmap
	kv.ClientIdBySerialNum = clientIdBySerialNum
}


// 我之前的做法是一个for loop 睡interval再继续， 每次loop all cached log看来的Index是否match存在
// 缺点是如果消息在Interval之内回来 我也继续睡。log会增长过大 没有效率.而且忘记测是否是leader！
// 现在的实现是一个general await api, 每隔Interval检查一下还是不是leader。 或者applied index committed消息抵达
// 如果是当初pass in的command, 直接trigger RPC handler
// 最后就是处理好正确或者错误的Case之后删除channel
func (kv *RaftKV) await(index int, op Op) (success bool) {
	kv.Lock()
	awaitChan := make(chan raft.ApplyMsg, 1)
	kv.requestHandlers[index] = awaitChan
	kv.UnLock()

	for {
		select {
		case message := <-awaitChan:
			kv.Lock()
			delete(kv.requestHandlers, index)
			kv.UnLock()

			if index == message.CommandIndex && op == message.Command {
				return true
			} else { 
				// Message at index was not what we're expecting, must not be leader in majority partition
				return false
			}
		case <- time.After(AwaitLeaderCheckInterval):
			kv.Lock()
			if _, isLeader := kv.rf.GetState(); !isLeader { 
				// We're no longer leader. Abort
				delete(kv.requestHandlers, index)
				kv.UnLock()
				return false
			}
			kv.UnLock()
		}
	}
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	ops := Op {
		Method : "Get",
		Key : args.Key,
		ClientId : args.ClientId,
		SerialNum : args.SerialNum,
	}

	//kv.Lock()
	index, _, isLeader := kv.rf.Start(ops)
	//kv.UnLock()

	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := kv.await(index, ops)
		if !success {
			reply.WrongLeader = true
		} else {
			kv.Lock()
			reply.WrongLeader = false			

			//log.Println(kv.me, "Get I got you", ops)
			if val, ok := kv.Kvmap[args.Key]; ok {
				reply.Value = val
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
			kv.UnLock()
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	ops := Op{
		Method : args.Op,
		Key : args.Key,
		Value : args.Value,
		ClientId : args.ClientId,
		SerialNum : args.SerialNum,
	}
	
	//kv.Lock()
	index, _, isLeader := kv.rf.Start(ops)
	//kv.UnLock()

	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := kv.await(index, ops)
		if !success {
			reply.WrongLeader = true
		} else {
			kv.Lock()
			//log.Println(kv.me, "PutAppend I got you", ops)
			reply.WrongLeader = false
			reply.Err = OK
			kv.UnLock()
		}

	}
}

func (kv *RaftKV) raftStateSizeHitThreshold() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	raftSize := kv.rf.GetRaftStateSize()
	if raftSize >= kv.maxraftstate {//* 100 / 95 {
		return true
	}
	return false
}

func (kv *RaftKV) periodCheckApplyMsg() {
	for m := range kv.applyCh {
		kv.Lock()

		if kv.isDecommissioned {
			kv.UnLock()
			return
		}

		// ApplyMsg might be a request to load snapshot
		if m.UseSnapshot { 
			kv.loadSnapshot(m.Snapshot)
			kv.UnLock()
			continue
		}
		
		ops := m.Command.(Op)

		// if we never process this client, or we never process this operation serial number
		// then we have a new request, we need to process it
		// Get request we do not care, handler will do the fetch.
		// For Put or Append, we do it here.
		// Alternatively, each RPC handler will have the following logic
		if serialN, ok := kv.ClientIdBySerialNum[ops.ClientId]; !ok || serialN != ops.SerialNum {
			// save the client id and its serial number
			kv.ClientIdBySerialNum[ops.ClientId] = ops.SerialNum
			if ops.Method == "Put" {
				kv.Kvmap[ops.Key] = ops.Value
			} else if ops.Method == "Append" {
				kv.Kvmap[ops.Key] += ops.Value
			}
		}

		//log.Println(kv.rf.Me(), "got applied message", m)
		
		// When we have applied message, we found the waiting channel(issued by RPC handler), forward the Ops
		if c, ok := kv.requestHandlers[m.CommandIndex]; ok {
			c <- m
		}

		// Whenever key/value server detects that the Raft state size is approaching this threshold, 
		// it should save a snapshot, and tell the Raft library that it has snapshotted, 
		// so that Raft can discard old log entries. 
		if kv.snapshotsEnabled && kv.raftStateSizeHitThreshold() {
			//log.Println("we snapshot!")
			kv.createSnapshot(m.CommandIndex)
		}

		kv.UnLock()
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	//kv.Lock()
	//defer kv.UnLock()

	kv.rf.Kill()
	kv.isDecommissioned = true
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
	kv.snapshotsEnabled = (maxraftstate != -1)

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.Kvmap = make(map[string]string)
	kv.ClientIdBySerialNum = make(map[int64]int64)

	kv.requestHandlers = make(map[int]chan raft.ApplyMsg)

	if data := persister.ReadSnapshot(); kv.snapshotsEnabled && data != nil && len(data) > 0 {
		kv.loadSnapshot(data)
	}

	go kv.periodCheckApplyMsg()

	return kv
}
