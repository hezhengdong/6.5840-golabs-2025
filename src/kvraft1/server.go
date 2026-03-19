package kvraft

import (
	"bytes"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"

)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	data map[string]DataValue
}

type DataValue struct {
	Value string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any { // 串行执行的，应该不用加锁
	// Your code here
	var res any
	switch args := req.(type) { // 基本就是把 Get Put 的处理逻辑照搬了过来
	case rpc.GetArgs:
		dataValue, exist := kv.data[args.Key]
		reply := rpc.GetReply{}
		if exist {
			reply.Value = dataValue.Value
			reply.Version = dataValue.Version
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		res = reply
	case rpc.PutArgs:
		dataValue, exist := kv.data[args.Key]
		reply := rpc.PutReply{}
		if exist {
			if args.Version == dataValue.Version {
				args.Version++
				kv.data[args.Key] = DataValue{
					Value: args.Value,
					Version: args.Version,
				}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		} else {
			if args.Version == 0 {
				args.Version++
				kv.data[args.Key] = DataValue{
					Value: args.Value,
					Version: args.Version,
				}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
		}
		res = reply
	default:
		panic("未知情况")
	}

	return res
}

// RSM 调用，将 KV 数据序列化为字节数组
func (kv *KVServer) Snapshot() []byte {
	// Your code here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	snapshot := w.Bytes()
	return snapshot
}

// RSM 调用，将字节数组反序列化为 KV 数据
func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var data1 map[string]DataValue
	if d.Decode(&data1) != nil {
		panic("解码失败")
	} else {
		kv.data = data1 // 原来的数据被 GC
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	err, res := kv.rsm.Submit(*args) // 返回执行情况、执行结果
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
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, data: make(map[string]DataValue)}


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
