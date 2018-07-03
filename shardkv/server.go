package shardkv


import (
	"kvdb/shardmaster"
	"kvdb/labrpc"
	"kvdb/raft"
	"sync"
	"kvdb/labgob"
	"time"
	"bytes"
	"log"
)

type CommandType int
const (
	Put CommandType = iota
	Append
	Get
	ExportShard
	ImportShard
)

type MigrationProcess int
const (
	Completed  MigrationProcess = iota
	Export
	Import
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method string
	Key string
	Value string
	ClientId int64
	SerialNum int	

	// used for import shards, followers need to catch up the latest changes
	Kvmap map[string]string
	Duplicate map[int64]int

	// used for import and export
	ShardNumber int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	mck *shardmaster.Clerk

	// Your definitions here.
	snapshotsEnabled bool
	isDecommissioned bool

	// Your definitions here.
	snapshotIndex int

	Kvmap map[string]string

	// duplication detection table
	duplicate map[int64]int

	requestHandlers map[int]chan raft.ApplyMsg

	Shards [shardmaster.NShards]int
	StatusByShards map[int]MigrationProcess

	LatestCfg shardmaster.Config
}

const AwaitLeaderCheckInterval = 10 * time.Millisecond
const SnapshotSizeTolerancePercentage = 5

func (kv *ShardKV) Lock() {
	kv.mu.Lock()
}

func (kv *ShardKV) UnLock() {
	kv.mu.Unlock()
}

func (kv *ShardKV) createSnapshot(logIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.snapshotIndex = logIndex

	e.Encode(kv.Kvmap)
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.duplicate)
	data := w.Bytes()
	kv.rf.SaveSnapShot(data)

	// Compact raft log til index.
	kv.rf.CompactLog(logIndex)
}

func (kv *ShardKV) loadSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	kvmap := make(map[string]string)
	duplicate := make(map[int64]int)
	d.Decode(&kvmap)
	d.Decode(&kv.snapshotIndex)
	d.Decode(&duplicate)

	kv.Kvmap = kvmap
	kv.duplicate = duplicate
}


// 我之前的做法是一个for loop 睡interval再继续， 每次loop all cached log看来的Index是否match存在
// 缺点是如果消息在Interval之内回来 我也继续睡。log会增长过大 没有效率.而且忘记测是否是leader！
// 现在的实现是一个general await api, 每隔Interval检查一下还是不是leader。 或者applied index committed消息抵达
// 如果是当初pass in的command, 直接trigger RPC handler
// 最后就是处理好正确或者错误的Case之后删除channel
func (kv *ShardKV) await(index int, op Op) (success bool) {
	kv.Lock()	
	awaitChan, ok := kv.requestHandlers[index]
	if !ok {
		awaitChan = make(chan raft.ApplyMsg, 1)
		kv.requestHandlers[index] = awaitChan
	} else {
		log.Println("why again?????????")
		return true
	}
	
	kv.UnLock()

	for {
		select {
		case message := <-awaitChan:
			if index == message.CommandIndex {
				close(awaitChan)
				return true
			} else { 
				// Message at index was not what we're expecting, must not be leader in majority partition
				return false
			}
		case <-time.After(800 * time.Millisecond):
			return false
		}
	}
}

func (kv *ShardKV) checkIfOwnsKey(key string) bool {
	// check if I owns the shard of the key
	//log.Println("Current shards map", kv.Shards)
	shardNum := key2shard(key)
	if gid := kv.Shards[shardNum]; gid != kv.gid {
		//log.Println(kv.me, "we DOES NOT OWN shard", shardNum, "for key", key)
		return false
	} else {
		//log.Println(kv.me, "we owns shard", shardNum, "for key", key, "our gid is", kv.gid,
		//	"caculated gid is", gid)
		return true
	}
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.WrongLeader = true
	reply.Err = ""
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	kv.Lock()

	if !kv.checkIfOwnsKey(args.Key) {
		kv.UnLock()		
		reply.Err = ErrWrongGroup
		return
	}

	if dup, ok := kv.duplicate[args.ClientId]; ok {
		// filter duplicate
		if args.SerialNum <= dup {
			kv.UnLock()
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = kv.Kvmap[args.Key]
			log.Println("duplicate??")
			return
		}
	}

	ops := Op {
		Method : "Get",
		Key : args.Key,
		ClientId : args.ClientId,
		SerialNum : args.SerialNum,
	}
	kv.UnLock()
	
	index, _, isLeader := kv.rf.Start(ops)

	if !isLeader {
		reply.WrongLeader = true
		//log.Println("Wrong leader1111!!!Get: my current kvmap", kv.Kvmap, "ops is", ops)
	} else {
		success := kv.await(index, ops)
		if !success {
			reply.WrongLeader = true
			//log.Println("Wrong leader2222!!!Get: my current kvmap", kv.Kvmap, "ops is", ops)
		} else {
			kv.Lock()
			reply.WrongLeader = false			

			//log.Println("Get: my current kvmap", kv.Kvmap, "ops is", ops)
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.WrongLeader = true
	reply.Err = ""
	if _, isLeader := kv.rf.GetState(); !isLeader {		
		return
	}

	kv.Lock()

	if !kv.checkIfOwnsKey(args.Key) {
		kv.UnLock()		
		reply.Err = ErrWrongGroup
		return
	}

	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientId]; ok {
		// filter duplicate
		if args.SerialNum <= dup {
			kv.UnLock()
			reply.WrongLeader = false
			reply.Err = OK
			return
		}
	}

	ops := Op{
		Method : args.Op,
		Key : args.Key,
		Value : args.Value,
		ClientId : args.ClientId,
		SerialNum : args.SerialNum,
	}
	kv.UnLock()

	index, _, isLeader := kv.rf.Start(ops)
	
	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := kv.await(index, ops)
		//log.Println("Put: my current kvmap", kv.Kvmap, "ops is", ops)
		if !success {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
		}

	}
}

func (kv *ShardKV) raftStateSizeHitThreshold() bool {
	if kv.maxraftstate < 0 {
		return false
	}
	if kv.maxraftstate < kv.rf.GetRaftStateSize() {
		return true
	}
	// abs < 10% of max
	var abs = kv.maxraftstate - kv.rf.GetRaftStateSize()
	var threshold = kv.maxraftstate / 10
	if abs < threshold {
		return true
	}
	return false
}

func (kv *ShardKV) periodCheckApplyMsg() {
	for {
		select {
		case m, ok := <-kv.applyCh:
			if ok {
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

				cmd := m.Command.(Op)

				// if command is MigrateShard, update owning shards by deletion

				// if command is MigrationComplete, update owning shards by changing Import to Completed.


				if m.Command != nil && m.CommandIndex > kv.snapshotIndex {					

					// if we never process this client, or we never process this operation serial number
					// then we have a new request, we need to process it
					// Get request we do not care, handler will do the fetch.
					// For Put or Append, we do it here.
					if dup, ok := kv.duplicate[cmd.ClientId]; !ok || dup < cmd.SerialNum {
						// save the client id and its serial number
						switch cmd.Method {
						case "Get":
							kv.duplicate[cmd.ClientId] = cmd.SerialNum
						case "Put":
							kv.Kvmap[cmd.Key] = cmd.Value
							kv.duplicate[cmd.ClientId] = cmd.SerialNum
						case "Append":
							kv.Kvmap[cmd.Key] += cmd.Value
							kv.duplicate[cmd.ClientId] = cmd.SerialNum
						}
					}
					
					// When we have applied message, we found the waiting channel(issued by RPC handler), forward the Ops
					if c, ok := kv.requestHandlers[m.CommandIndex]; ok {
						c <- m
						delete(kv.requestHandlers, m.CommandIndex)
					}

					// Whenever key/value server detects that the Raft state size is approaching this threshold, 
					// it should save a snapshot, and tell the Raft library that it has snapshotted, 
					// so that Raft can discard old log entries. 
					if kv.snapshotsEnabled && kv.raftStateSizeHitThreshold() {
						kv.createSnapshot(m.CommandIndex)
					}
				}
				kv.UnLock()
			}
		}

	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.isDecommissioned = true
}

func (kv *ShardKV) periodCheckShardMasterConfig() {

	for {
		cfg := kv.mck.Query(-1)
		kv.Lock()

		kv.LatestCfg = cfg

		if _, isLeader := kv.rf.GetState(); isLeader {
			// check what shards we have lost
			for i, gid := range kv.Shards {
				if gid == kv.gid {
					curGroupdId := cfg.Shards[i]
					if gid != curGroupdId && curGroupdId != 0 {
						log.Println(kv.me, "lost shard", i, "from group id", kv.gid, "new owner is", curGroupdId)
						kv.sendMigrateShard(i, curGroupdId)
					}
				}
			}
		}
		
		kv.Shards = cfg.Shards
		
		kv.UnLock()

		time.Sleep(100 * time.Millisecond)
	}
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.Kvmap = make(map[string]string)
	kv.duplicate = make(map[int64]int)

	kv.requestHandlers = make(map[int]chan raft.ApplyMsg)

	if data := persister.ReadSnapshot(); kv.snapshotsEnabled && data != nil && len(data) > 0 {
		kv.loadSnapshot(data)
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.periodCheckShardMasterConfig()
	go kv.periodCheckApplyMsg()

	return kv
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {

	if _, isLeader := kv.rf.GetState(); !isLeader {	
		reply.WrongLeader = true	
		return
	}

	log.Println(kv.gid, "got migration for exporting shard", args.ShardNumber)


	// if already handled, return

	// save kvmap, duplicate map, but do not change shard ownership to ourselves

	// change shard status to Import

	// start ops command to let followers know we are ready to take in new requests
}

func (kv *ShardKV) sendMigrateShard(shard int, destGid int) {
	req := MigrateShardArgs{
		//ConfigVersion: kv.
		ShardNumber: shard,
		Kvmap : make(map[string]string),
		Duplicate : make(map[int64]int),
	}

	// copy kvmap, and duplicate map
	for k,v := range kv.Kvmap {
		req.Kvmap[k] = v
	}
	for k,v := range kv.duplicate {
		req.Duplicate[k] = v
	}

	// set our status to export

	// start ops command to let peer know we export shards

	// send RPC to destination

	// for each server in gid, call it until found a leader and success
	if servers, ok := kv.LatestCfg.Groups[destGid]; ok {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply MigrateShardReply
			ok := srv.Call("ShardKV.MigrateShard", &req, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				//log.Println("Got result from", si)
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				log.Println("Something is wrong, the group does not have the shards we thought")
				break
			}
		}
	}

}