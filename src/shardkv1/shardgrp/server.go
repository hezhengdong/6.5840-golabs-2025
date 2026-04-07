package shardgrp

import (
	"bytes"
	"sync"
	"sync/atomic"


	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const (
	NShards  = 12
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	shards  [NShards]Shard
	mu      sync.RWMutex
}

type Shard struct {
	Owned   bool
	Version shardcfg.Tnum // 分片分组配置的版本号（版本号的基本单位是 shard，而不是 group）
	KVStore map[string]DataValue
}

type DataValue struct {
	Value string
	Version rpc.Tversion
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 能够进入这里的操作，可以确保一定持有对应分片，因为在 Get/Put 中 ErrWrongGroup 就已被过滤
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.applyGet(&args)
	case rpc.PutArgs:
		return kv.applyPut(&args)
	case shardrpc.FreezeShardArgs:
		return kv.applyFreeze(&args)
	case shardrpc.InstallShardArgs:
		return kv.applyInstall(&args)
	case shardrpc.DeleteShardArgs:
		return kv.applyDelete(&args)
	default:
		panic("未知情况")
	}
}


func (kv *KVServer) Snapshot() []byte {
	// Your code here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	snapshot := w.Bytes()
	return snapshot
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shards [NShards]Shard
	if d.Decode(&shards) != nil {
		panic("解码失败")
	} else {
		kv.shards = shards // 原来的数据被 GC
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	switch err {
	case rpc.ErrWrongLeader:
		reply.Err = rpc.ErrWrongLeader
	case rpc.OK:
		*reply = res.(rpc.GetReply)
	default:
		panic("未知情况")
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	switch err {
	case rpc.ErrWrongLeader:
		reply.Err = rpc.ErrWrongLeader
	case rpc.OK:
		*reply = res.(rpc.PutReply)
	default:
		panic("未知情况")
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	switch err {
	case rpc.ErrWrongLeader:
		reply.Err = rpc.ErrWrongLeader
	case rpc.OK:
		*reply = res.(shardrpc.FreezeShardReply)
	default:
		panic("未知情况")
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	switch err {
	case rpc.ErrWrongLeader:
		reply.Err = rpc.ErrWrongLeader
	case rpc.OK:
		*reply = res.(shardrpc.InstallShardReply)
	default:
		panic("未知情况")
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	switch err {
	case rpc.ErrWrongLeader:
		reply.Err = rpc.ErrWrongLeader
	case rpc.OK:
		*reply = res.(shardrpc.DeleteShardReply)
	default:
		panic("未知情况")
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	for i := 0; i < NShards; i++ {
		kv.shards[i] = Shard{
			Owned: gid == 1, // 根据 lab 文档，初始化时，分片全部在 group 1 中
			Version: 0, // 初始版本号应该是多少呢？似乎只要尽可能小就没问题
			KVStore: make(map[string]DataValue),
		}
	}

	return []tester.IService{kv, kv.rsm.Raft()}
}
