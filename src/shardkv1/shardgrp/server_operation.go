package shardgrp

import (
	"bytes"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

func (kv *KVServer) applyGet(args *rpc.GetArgs) rpc.GetReply {
	shardId := shardcfg.Key2Shard(args.Key)
	reply := rpc.GetReply{}
	// 先检查自己是否掌管着相关分片
	if !kv.shards[shardId].Owned {
		reply.Err = rpc.ErrWrongGroup
		raft.Debugf(raft.DServer, "G%v -> G0 Get {err: %v} 分组不持有分片 %v", kv.gid, reply.Err, shardId)
		return reply
	}

	// 随后进行正常的 Get 逻辑
	dataValue, exist := kv.shards[shardId].KVStore[args.Key]
	if exist {
		reply.Value = dataValue.Value
		reply.Version = dataValue.Version
		reply.Err = rpc.OK
		raft.Debugf(raft.DServer, "G%v -> G0 Get 操作执行成功, {value: %v, version: %v}", kv.gid, reply.Value, reply.Version)
	} else {
		reply.Err = rpc.ErrNoKey
		raft.Debugf(raft.DServer, "G%v -> G0 Get 操作执行失败, {err: %v}", kv.gid, reply.Err)
	}

	return reply
}

func (kv *KVServer) applyPut(args *rpc.PutArgs) rpc.PutReply {
	shardId := shardcfg.Key2Shard(args.Key)
	reply := rpc.PutReply{}
	// 先检查自己是否掌管着相关分片
	if !kv.shards[shardId].Owned {
		reply.Err = rpc.ErrWrongGroup
		raft.Debugf(raft.DServer, "G%v -> G0 Put {err: %v} 分组不持有分片 %v", kv.gid, reply.Err, shardId)
		return reply
	}

	// 随后进行正常的 Put 逻辑
	dataValue, exist := kv.shards[shardId].KVStore[args.Key]
	if exist {
		if args.Version == dataValue.Version {
			args.Version++
			kv.shards[shardId].KVStore[args.Key] = DataValue{
				Value: args.Value,
				Version: args.Version,
			}
			reply.Err = rpc.OK
			raft.Debugf(raft.DServer, "G%v -> G0 Put 操作执行成功", kv.gid)
		} else {
			reply.Err = rpc.ErrVersion
			raft.Debugf(raft.DServer, "G%v -> G0 Put 操作执行失败, {err: %v}", kv.gid, reply.Err)
		}
	} else {
		if args.Version == 0 {
			args.Version++
			kv.shards[shardId].KVStore[args.Key] = DataValue{
				Value: args.Value,
				Version: args.Version,
			}
			reply.Err = rpc.OK
			raft.Debugf(raft.DServer, "G%v -> G0 Put 操作执行成功", kv.gid)
		} else {
			reply.Err = rpc.ErrNoKey
			raft.Debugf(raft.DServer, "G%v -> G0 Put 操作执行失败, {err: %v}", kv.gid, reply.Err)
		}
	}
	return reply
}

func (kv *KVServer) applyFreeze(args *shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {

	reply := shardrpc.FreezeShardReply{}

	// 具体的冻结逻辑
	// 1. 先对比配置版本号与自己持有的版本号
	// 1.1 如果 args 版本号小于自己的版本号，丢弃该请求
	shardId := args.Shard
	shard := &kv.shards[shardId]
	if args.Num < shard.Version {
		raft.Debugf(raft.DShard, "G%v Freeze 丢弃请求", kv.gid)
		reply.State = nil
		reply.Num = args.Num
		reply.Err = rpc.OK
		return reply
	}
	// 1.2 如果 args 版本号大于自己的版本号，将本次请求视为正常请求
	if args.Num > shard.Version {
		raft.Debugf(raft.DShard, "G%v Freeze 正常请求", kv.gid)
		// 更新版本号
		shard.Version = args.Num
		// 冻结分片
		shard.Owned = false
		raft.Debugf(raft.DShard, "G%v Freeze 成功设置分片 %v 为非自己持有 {shard.Owned: %v}", kv.gid, shardId, shard.Owned)
		// 序列化数据
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(shard.KVStore)
		shardBytes := w.Bytes()
		// 组装返回值
		reply.State = shardBytes
		reply.Num = shard.Version
		reply.Err = rpc.OK
	}
	// 1.3 如果 args 版本号等于自己的版本号，不修改属性，但是会正常返回，因为这可能是网络的问题
	if args.Num == shard.Version {
		raft.Debugf(raft.DShard, "G%v Freeze 重复请求", kv.gid)
		// 序列化数据
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(shard.KVStore)
		shardBytes := w.Bytes()
		// 组装返回值
		reply.State = shardBytes
		reply.Num = shard.Version
		reply.Err = rpc.OK
	}

	return reply
}

func (kv *KVServer) applyInstall(args *shardrpc.InstallShardArgs) shardrpc.InstallShardReply {

	reply := shardrpc.InstallShardReply{}

	// 这里就是接收数据，反序列化为对应的数据结构

	// 1. 依旧是检查版本三步走
	// 1.1 小于，丢弃请求
	shardId := args.Shard
	shard := &kv.shards[shardId]
	if args.Num < shard.Version || args.State == nil {
		raft.Debugf(raft.DShard, "G%v Install 丢弃请求", kv.gid)
		//
		reply.Err = rpc.OK
		return reply
	}
	// 1.2 大于
	if args.Num > shard.Version {
		raft.Debugf(raft.DShard, "G%v Install 正常请求", kv.gid)
		// 1. 更新版本号
		shard.Version = args.Num
		// 2. 设置该分片为自己所有
		shard.Owned = true
		raft.Debugf(raft.DShard, "G%v Install 成功设置分片 %v 为自己持有 {shard.Owned: %v}", kv.gid, shardId, shard.Owned)
		// 3. 反序列化数据，将数据存储到分片对应数据结构中
		r := bytes.NewBuffer(args.State)
		d := labgob.NewDecoder(r)
		var kvstore map[string]DataValue
		if d.Decode(&kvstore) != nil {
			panic("解码失败")
		} else {
			shard.KVStore = kvstore
		}
		// 4. 组装返回值
		reply.Err = rpc.OK
	}
	// 1.3 等于
	if args.Num == shard.Version {
		raft.Debugf(raft.DShard, "G%v Install 重复请求", kv.gid)
		// 如果是等于，那就是因网络问题，导致 rpc 被重复发送
		// 这种情况，一般数据已经被先前的 rpc 处理了，因此直接返回 rpc.OK 即可
		reply.Err = rpc.OK
	}

	return reply
}

func (kv *KVServer) applyDelete(args *shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {

	reply := shardrpc.DeleteShardReply{}

	// 这里就是删除被冻结的分片，清空其中的 KV 数据

	// 1. 依旧是检查版本三步走。但是 delete 相比较 freeze install 更为特殊
	// 1.1 配置版本号大于分片版本号，理论上绝对不会发生，直接panic吧
	shardId := args.Shard
	shard := &kv.shards[shardId]
	if args.Num > shard.Version {
		panic("未知情况")
	}
	// 1.2 配置版本号小于分片版本号
	if args.Num < shard.Version {
		raft.Debugf(raft.DShard, "G%v Delete 过期请求", kv.gid)
		// 过期请求，直接返回rpc.OK吧
		reply.Err = rpc.OK
	}
	// 1.3 配置版本号等于分片版本号
	if args.Num == shard.Version {
		raft.Debugf(raft.DShard, "G%v Delete 正常请求", kv.gid)
		// 正常情况，也有可能是重复请求
		// 删除数据
		shard.KVStore = make(map[string]DataValue)
		shard.Owned = false // 按理说这行代码是多余的，一套的 freeze 和 delete 间也不太可能穿插其他操作
		raft.Debugf(raft.DShard, "G%v Delete 成功设置分片 %v 为非自己持有 {shard.Owned: %v}", kv.gid, shardId, shard.Owned)
		reply.Err = rpc.OK
	}

	return reply
}