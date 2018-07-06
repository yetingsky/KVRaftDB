package raftkv

import (
	"time"
	//"log"
	"kvdb/labrpc"
	"crypto/rand"
	"math/big"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	clientId int64
	serialNum int64
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
	// You'll have to add code here.
	ck.lastLeader = 0

	ck.clientId = nrand()
	ck.serialNum = 0
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
	// You will have to modify this function.
	cnt := len(ck.servers)
	for {
		args := GetArgs {
			Key : key,
			ClientId : ck.clientId,
			SerialNum : ck.serialNum,
		}
		reply := GetReply{}

		ck.lastLeader %= cnt
		done := make(chan bool, 1)
		go func() {
			ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", &args, &reply)
			done <- ok
		}()
		select {
		case <-time.After(150 * time.Millisecond): // rpc timeout: 200ms
			ck.lastLeader++
			continue
		case ok := <-done:
			if ok && !reply.WrongLeader {
				//ck.serialNum++
				if reply.Err == OK {
					return reply.Value
				}
				return ""
			}
			ck.lastLeader++
		}
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
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	cnt := len(ck.servers)
	for {
		args := PutAppendArgs {
			Key : key,
			Value : value,
			Op : op,
			ClientId : ck.clientId,
			SerialNum : ck.serialNum,
		}
		reply := GetReply{}

		ck.lastLeader %= cnt
		done := make(chan bool, 1)
		go func() {
			ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", &args, &reply)
			done <- ok
		}()
		select {
		case <-time.After(300 * time.Millisecond): // rpc timeout: 200ms
			ck.lastLeader++
			continue
		case ok := <-done:
			if ok && !reply.WrongLeader && reply.Err == OK {
				ck.serialNum = nrand()
				return
			}
			ck.lastLeader++
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
