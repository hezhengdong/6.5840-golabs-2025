package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

// 该文件是真正的面相用户的客户端

import (
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	// You'll have to add code here.
	return ck
}


// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.

	for {

		// 找到 key 所属的分片
		shardId := shardcfg.Key2Shard(key)

		// 获取系统配置
		cfg := ck.sck.Query()

		// 根据配置找到 shard 的 group id
		groupId, servers, exist := cfg.GidServers(shardId)
		if !exist {
			panic(fmt.Sprintf("ShardConfig 中不存在 groupId: %v", groupId))
		}

		// lab 文档说每次请求创建 Client，感觉不是很优雅，应该缓存起来的
		clerk := shardgrp.MakeClerk(ck.clnt, servers)

		// 发起 Get 请求
		raft.Debugf(raft.DClient, "G0 -> G%v Get {key: %v} {cfg: %v}", groupId, key, cfg)
		value, version, err := clerk.Get(key)

		// 如果分组或配置错误，读取新配置重试
		if err == rpc.ErrWrongGroup || err == rpc.ErrConfig {
			raft.Debugf(raft.DClient, "G0 <- G%v Get {err: %v}", groupId, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	// 这里简直是究极大坑，为什么官方要拆分两个 Client？对于 Put ErrMaybe 来说，容易被 corner case 气死……
	resend := false
	// 官方的做法太不优雅了
	// 如果内部死循环，万一读到旧配置，rpc 机器压根就不可能成功，就会陷入死循环
	// 如果给循环限定次数，还有 ErrMaybe 这一大问题。循环次数结束后，万一实际成功，但是响应数据包丢失怎么办？
	// 自己的解决方法是，外层通过 resend 处理 ErrMaybe 的问题。但这样导致代码的可读性太差，耦合度过高
	for {
		shardId := shardcfg.Key2Shard(key)
		cfg := ck.sck.Query()
		groupId, servers, exist := cfg.GidServers(shardId)
		if !exist {
			panic(fmt.Sprintf("ShardConfig 中不存在 groupId: %v", groupId))
		}

		clerk := shardgrp.MakeClerk(ck.clnt, servers)
		raft.Debugf(raft.DClient, "G0 -> G%v Put {key: %v, value: %v, version: %v} {cfg: %v}", groupId, key, value, version, cfg)
		err := clerk.Put(key, value, version)

		if err == rpc.ErrWrongGroup {
			raft.Debugf(raft.DClient, "G0 <- G%v Put {err: %v}", groupId, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if err == rpc.ErrConfig {
			raft.Debugf(raft.DClient, "G0 <- G%v Put {err: %v}", groupId, err)
			time.Sleep(100 * time.Millisecond)
			resend = true
			continue
		}

		if err == rpc.ErrVersion && resend {
            return rpc.ErrMaybe
        }

		return err
	}
}
