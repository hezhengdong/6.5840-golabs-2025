package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)


// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

const (
	CurrentConfig = "current_config"
	NextConfig = "next_config"
)

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// ErrMaybe 太烦人了，封装起来
func (sck *ShardCtrler) safePut(key string, value string, version rpc.Tversion) rpc.Err {
	for {
		err := sck.IKVClerk.Put(key, value, version)

		// 明确的 OK 和 ErrVersion 可以返回给上层
		if err == rpc.OK || err == rpc.ErrVersion {
			return err
		}

		if err == rpc.ErrMaybe {
			// 消除 ErrMaybe 的解决方法：客户端记录状态
			currentVal, _, getErr := sck.IKVClerk.Get(key)
			if getErr != rpc.OK {
				panic("未知情况") // Get 按理说不可能失败，如果失败直接 panic 吧
			}

			if currentVal == value {
				return rpc.OK
			}

			// 如果判断 ErrMaybe 为 ErrVersion，重试，或者也可以直接返回
		}
	}
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	// 获取两次配置
	cur, _, err1 := sck.IKVClerk.Get(CurrentConfig)
	nxt, _, err2 := sck.IKVClerk.Get(NextConfig)
	// 如果没有 NextConfig，跳过
	if err1 == rpc.ErrNoKey || err2 == rpc.ErrNoKey {
		return
	}
	// 如果 NextConfig 的版本号等于 CurrentConfig 的版本号，说明已被成功执行，跳过
	if shardcfg.FromString(nxt).Num <= shardcfg.FromString(cur).Num {
		return
	} else { // 否则进行分片迁移
		nextConfig := shardcfg.FromString(nxt)
		sck.ChangeConfigTo(nextConfig)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	str := cfg.String()
	err := sck.safePut(CurrentConfig, str, 0)
	if err != rpc.OK {
		panic("未知情况")
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) { // 测试程序已经递增版本号了
	// Your code here.

	// 1. 写入 NextConfig（用于崩溃恢复）；如果同版本有多个控制器进入，同步配置
	// 解释：由于测试程序的要求，在 ChangeConfigTo 执行完毕后，分片迁移必须完成
	// 因此，必须允许多个同版本号的控制器同时执行分片迁移，每个控制器的配置必须一致
	var err rpc.Err
	for {
		nxtStr, version, errGet := sck.IKVClerk.Get(NextConfig)
		if errGet == rpc.OK {
			nxtCfg := shardcfg.FromString(nxtStr)
			if nxtCfg.Num == new.Num {
				// 如果版本号相同，说明自己来晚了，覆盖自己的配置，进入分片迁移逻辑
				new = nxtCfg
				break
			} else if nxtCfg.Num > new.Num {
				// 如果版本号过小，说明当前版本的分片迁移已执行完毕，直接返回
				return
			}
		}

		// 如果自己是当前版本下进度最快的控制器，写入 NextConfig
		err = sck.safePut(NextConfig, new.String(), version)
		if err != rpc.ErrVersion {
			break
		}
		// 如果返回 ErrVersion，说明 Put 操作被别人抢先，重试
		time.Sleep(10 * time.Millisecond)
	}

	// 2. 获取 CurrentConfig，用于与新配置做对比
	old := sck.Query()
	if old.Num >= new.Num {
		return
	}

	// 3. 对比配置，找到「哪些分片需要从哪个分组迁移到另一个分组」，并执行分片迁移
	for shardId := 0; shardId < shardcfg.NShards; shardId++ {
		oldGroupId := old.Shards[shardId]
		newGroupId := new.Shards[shardId]
		// 如果二者不同，说明分片需要进行迁移
		if oldGroupId != newGroupId {
			// 课程对于 raft 成员变更一点也没提，应该不需要考虑吧（恼）
			oldClient := shardgrp.MakeClerk(sck.clnt, old.Groups[oldGroupId])
			newClient := shardgrp.MakeClerk(sck.clnt, new.Groups[newGroupId])

			raft.Debugf(raft.DShard, "G0 将分片 %v 从 G%v 迁移到 G%v", shardId, oldGroupId, newGroupId)
			// 冻结旧组分片
			raft.Debugf(raft.DShard, "G0 -> G%v Freeze Shard {shardId: %v, version: %v}", oldGroupId, shardId, new.Num)
			shardBytes, errFreeze := oldClient.FreezeShard(shardcfg.Tshid(shardId), new.Num)
			// 为新组安装分片
			raft.Debugf(raft.DShard, "G0 -> G%v Install Shard {shardId: %v, len(bytes): %v, version: %v}", newGroupId, shardId, len(shardBytes), new.Num)
			errInstall := newClient.InstallShard(shardcfg.Tshid(shardId), shardBytes, new.Num)
			// 删除旧组分片
			raft.Debugf(raft.DShard, "G0 -> G%v Delete Shard {shardId: %v, version: %v}", oldGroupId, shardId, new.Num)
			errDelete := oldClient.DeleteShard(shardcfg.Tshid(shardId), new.Num)

			if errFreeze != rpc.OK || errInstall != rpc.OK || errDelete != rpc.OK {
				// panic("未知情况")
				return
			}
		}
	}

	// 4. 分片迁移完毕后，更新 CurrentConfig
	// 4.1 获取 CurrentConfig
	oldCfgStr, version, errGet := sck.IKVClerk.Get(CurrentConfig)
	// 4.2 对比版本号，如果已经有先行者修改成功，直接返回
	if errGet == rpc.OK {
		oldCfg := shardcfg.FromString(oldCfgStr)
		if oldCfg.Num >= new.Num {
			raft.Debugf(raft.DConf, "G0 旧配置的版本号大于自己即将写入的版本号，说明已经有先行者修改成功，直接返回")
			return
		}
	}
	// 4.3 更新 CurrentConfig
	raft.Debugf(raft.DConf, "G0 安装新配置 {cfg: %v}, 替换旧配置 {cfg: %v}}]", new.String(), oldCfgStr)
	errPut := sck.safePut(CurrentConfig, new.String(), version)
	// 4.4 如果返回 ErrVersion，说明 Put 被其他控制器抢占成功，自己返回
	if errPut == rpc.ErrVersion {
		raft.Debugf(raft.DConf, "G0 Put 抢占失败，说明其他执行器抢占成功，返回")
		return
	}
}


// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	value, _, err := sck.IKVClerk.Get(CurrentConfig)
	if err == rpc.ErrNoKey {
		panic("未知情况")
	}
	return shardcfg.FromString(value)
}

