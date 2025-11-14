package kvsrv

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)


type Clerk struct {
	clnt   *tester.Clnt // 用于发送 RPC 请求
	server string // 目标服务器的标识
}

// 创建并返回一个新的 Clerk 实例
// 参数:
//   - clnt: RPC 客户端对象
//   - server: 服务器标识符
// 返回值: 实现了 IKVClerk 接口的 Clerk 实例，约束 Clerk 必须实现 Get Put 方法
func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get 获取指定键的当前值和版本。
// 如果键不存在，则返回 ErrNoKey 错误。
// 对于其他所有错误情况，该方法会持续重试直至成功。
//
// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.

	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	// 如果失败，循环重试，直到发送成功。根据 Lab2 task3 讲义内容，不需要考虑一直失败的情况。Put 同理
	ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)

	var count int

	for !ok {
		count++
		time.Sleep(100 * time.Millisecond)
		log.Printf("Get RPC 调用失败, 尝试重试第 %v 次. key: %v\n", count, key)
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok {
			log.Printf("Get RPC 重试 %v 次后调用成功, reply.Err: %v", count, reply.Err)
			return reply.Value, reply.Version, reply.Err
		}
	}

	return reply.Value, reply.Version, reply.Err
}

// Put 仅在版本号匹配时更新 key 和 value。
// 如果版本号不匹配，服务器应当返回 ErrVersion。
// 如果 Put 在第一次 RPC 请求时接收到 ErrVersion，Put 应当返回 ErrVersion，因为 Put 一定不是在服务器上被执行。
// 如果服务器在重发 RPC 时返回 ErrVersion，Put 必须向应用层返回 ErrMaybe，因为先前的 RPC 可能被服务器成功处理，但是响应丢失，Clerk 无法确定 Put 是否被执行。
//
// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	args := rpc.PutArgs{
		Key: key,
		Value: value,
		Version: version,
	}
	reply := rpc.PutReply{}

	ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)

	var count int

	for !ok {
		count++
		time.Sleep(100 * time.Millisecond)
		log.Printf("Put RPC 调用失败, 尝试重试第 %v 次. key: %v, value: %v, version: %v", count, key, value, version)
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrVersion {
				log.Printf("Put RPC 重试 %v 次后调用成功, reply.Err: %v", count, rpc.ErrMaybe)
				return rpc.ErrMaybe // Lab2 task3 关键: 重试后 Put RPC 返回的 ErrVersion 需要被视为 ErrMaybe
			}
			log.Printf("Put RPC 重试 %v 次后调用成功, reply.Err: %v", count, reply.Err)
			return reply.Err
		}
	}

	return reply.Err
}
