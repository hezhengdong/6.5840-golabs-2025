package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type DataValue struct {
	Value string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	Data map[string]DataValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.Data = make(map[string]DataValue)
	return kv
}

// 如果 args.Key 存在，Get 返回 args.Key 的 value 和 version。反之，Get 返回 ErrNoKey。
//
// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock() // 读操作也需要加锁，只有读读并发没问题，读写并发、写写并发有问题。可以使用读写锁优化
	dataValue, exist := kv.Data[args.Key] // 只对共享资源加锁
	kv.mu.Unlock()

	if exist {
		reply.Value = dataValue.Value
		reply.Version = dataValue.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// 如果版本匹配，更新 key 的 value。
// 如果版本不匹配，返回 ErrVersion。
// 如果 key 不存在，并且 args.Version 为 0，Put 安装 value，否则返回 ErrNoKey。
//
// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	dataValue, exist := kv.Data[args.Key]
	if exist {
		if args.Version == dataValue.Version { // key 存在 && 版本匹配
			args.Version++
			kv.Data[args.Key] = DataValue{
				Value: args.Value,
				Version: args.Version,
			}
			reply.Err = rpc.OK
		} else { // key 存在 && 版本不匹配
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 { // key 不存在 && 版本为零
			args.Version++
			kv.Data[args.Key] = DataValue{
				Value: args.Value,
				Version: args.Version,
			}
			reply.Err = rpc.OK
		} else { // key 不存在 && 版本不为零
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
