package shardgrp

import (
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leaderId = rand.Intn(len(servers)) // 因为 leaderId 没缓存，所以初始 leaderId 设置为随机
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here

	args := rpc.GetArgs{Key: key}
	serverId := ck.leaderId

	for i := 0; i < len(ck.servers) * 2; i++ {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Get", &args, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			serverId = (serverId + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		ck.leaderId = serverId
		return reply.Value, reply.Version, reply.Err
	}

	// 如果多次遍历后 RPC 依旧失败，可能是因为读到了旧配置，返回 ErrConfig 读取新配置重试
	return "", 0, rpc.ErrConfig
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here

	args := rpc.PutArgs{
		Key: key,
		Value: value,
		Version: version,
	}
	serverId := ck.leaderId

	var count int

	for i := 0; i < len(ck.servers) * 2; i++ {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			serverId = (serverId + 1) % len(ck.servers)
			count++
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// count 的作用：对于第一次 RPC 成功响应，如果返回值为
		// ErrVersion，那么说明不存在网络丢包，也意味着不存在 ErrMaybe
		if count != 0 && reply.Err == rpc.ErrVersion {
			return rpc.ErrMaybe
		}

		ck.leaderId = serverId
		return reply.Err
	}

	return rpc.ErrConfig
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here

	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num: num,
	}
	serverId := ck.leaderId

	timeout := time.After(5 * time.Second)

    for {
        reply := shardrpc.FreezeShardReply{}
        done := make(chan bool, 1)

        go func() {
            ok := ck.clnt.Call(ck.servers[serverId], "KVServer.FreezeShard", &args, &reply)
            done <- ok
        }()

        select {
        case <-timeout:
            // panic("Freeze RPC 超时")
			return nil, rpc.ErrConfig

        case ok := <-done:
            if !ok || reply.Err == rpc.ErrWrongLeader {
                serverId = (serverId + 1) % len(ck.servers)
                time.Sleep(10 * time.Millisecond)
				if !ok {
					raft.Debugf(raft.DShard, "G0 Freeze retry, cause by rpc failed %v", time.Now())
				} else {
					raft.Debugf(raft.DShard, "G0 Freeze retry, cause by ErrWrongLeader %v", time.Now())
				}
                continue
            }

            ck.leaderId = serverId
            return reply.State, reply.Err
        }
    }
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here

	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num: num,
	}
	serverId := ck.leaderId

	timeout := time.After(5 * time.Second)

    for {
        reply := shardrpc.InstallShardReply{}
        done := make(chan bool, 1)

        go func() {
            ok := ck.clnt.Call(ck.servers[serverId], "KVServer.InstallShard", &args, &reply)
            done <- ok
        }()

        select {
        case <-timeout:
            // panic("Install RPC 超时")
			return rpc.ErrConfig

        case ok := <-done:
            if !ok || reply.Err == rpc.ErrWrongLeader {
                serverId = (serverId + 1) % len(ck.servers)
                time.Sleep(10 * time.Millisecond)
				if !ok {
					raft.Debugf(raft.DShard, "G0 Install retry, cause by rpc failed %v", time.Now())
				} else {
					raft.Debugf(raft.DShard, "G0 Install retry, cause by ErrWrongLeader %v", time.Now())
				}
				continue
            }

            ck.leaderId = serverId
            return reply.Err
        }
    }
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here

	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num: num,
	}
	serverId := ck.leaderId

	timeout := time.After(5 * time.Second)

    for {
        reply := shardrpc.DeleteShardReply{}
        done := make(chan bool, 1)

        go func() {
            ok := ck.clnt.Call(ck.servers[serverId], "KVServer.DeleteShard", &args, &reply)
            done <- ok
        }()

        select {
        case <-timeout:
            // panic("Delete RPC 超时")
			return rpc.ErrConfig

        case ok := <-done:
            if !ok || reply.Err == rpc.ErrWrongLeader {
                serverId = (serverId + 1) % len(ck.servers)
                time.Sleep(10 * time.Millisecond)
				if !ok {
					raft.Debugf(raft.DShard, "G0 Delete retry, cause by rpc failed %v", time.Now())
				} else {
					raft.Debugf(raft.DShard, "G0 Delete retry, cause by ErrWrongLeader %v", time.Now())
				}
                continue
            }

            ck.leaderId = serverId
            return reply.Err
        }
    }
}
