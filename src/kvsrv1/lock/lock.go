package lock

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVVLClerk 是 k/v clerk 的 Go 接口：
	// 该接口隐藏了 ck 的具体 Clerk 类型，但保证 ck 支持 Put 和 Get 方法。
	// 当测试程序调用 MakeLock() 时，会传入 clerk。
	//
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	l string
	me string
}

var unlocked string = "unlocked"

// 测试程序调用 MakeLock() 并传入一个 k/v clerk；
// 你的代码能够执行 Put 或 Get 通过调用 lk.ck.Put() 或 lk.ck.Get()
//
// 使用 l 作为 key 来存储“锁状态”（你必须精确决定锁的状态是什么）
//
// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck: ck,
		l: l,
		me: kvtest.RandValue(8),
	}
	// You may add code here
	return lk
}

// 思路：服务端需要维护一个标记来判断分布式锁的持有与释放
// Acquire：
// - 如果 value == unlocked，尝试 Put，如果发生 ErrVersion，获取锁失败，循环重试
// - 如果 value != unlocked，获取锁失败，循环重试
// Release：
// - Acquire 之间一定是并发执行的，Release 之间一定是串行执行的，Acquire 与 Release 二者会并发执行，这点要额外关注

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.l)

		// 如果整个系统第一次请求分布式锁，构建最初的键值对
		if err == rpc.ErrNoKey {
			// 此处可能会有并发问题，多个进程执行这行代码。
			// 但是根据服务端的版本号机制，只会有一个客户端执行成功，此处并发不影响结果。
			log.Printf("%v 尝试初始化分布式锁\n", lk.me)
			lk.ck.Put(lk.l, unlocked, 0)
			continue
		}

		// 如果分布式锁已被其他客户端持有，循环尝试
		if value != unlocked {
			log.Printf("%v 发现分布式锁已被抢占, 循环尝试\n", lk.me)
			continue
		}

		// 如果分布式锁未被其他客户端持有，尝试持有锁。
		// 这行代码实际被多个客户端并发执行，
		// 版本号机制确保只有一个客户端会成功执行，根据返回值处理对应情况。
		log.Printf("%v 尝试抢占分布式锁\n", lk.me)
		err2 := lk.ck.Put(lk.l, lk.me, version)

		// 如果成功持有锁，结束方法
		if err2 == rpc.OK {
			log.Printf("%v 抢占分布式锁成功\n", lk.me)
			break
		}

		// 如果未成功抢占锁，循环尝试
		if err2 == rpc.ErrVersion {
			log.Printf("%v 抢占分布式锁失败, 循环尝试\n", lk.me)
			continue
		}

		if err2 == rpc.ErrMaybe {
			value, _, err := lk.ck.Get(lk.l)
			if err != rpc.OK {
				log.Fatalf("出现未预料的情况: Get 操作执行失败\n")
			}
			if value == lk.me {
				log.Printf("ErrMaybe 情况下, Put RPC 命令实际执行成功, %v 抢占分布式锁成功\n", lk.me)
				break
			} else {
				log.Printf("ErrMaybe 情况下, Put RPC 命令实际执行失败, %v 抢占分布式锁失败, 循环尝试\n", lk.me)
				continue
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	// 根据分布式锁的逻辑，Release 方法一定会被串行执行，不存在并发问题。错误检测预防疏漏
	for {
		value, version, err := lk.ck.Get(lk.l)
		if value != lk.me {
			log.Fatalf("出现未预料的情况: 分布式锁未被当前进程 %v 持有, 而是 %v\n", lk.me, value)
		}
		if err != rpc.OK {
			log.Fatalf("出现未预料的情况: Get 操作执行失败\n")
		}
		// 尝试释放锁
		log.Printf("%v 尝试释放分布式锁\n", lk.me)
		err2 := lk.ck.Put(lk.l, unlocked, version)
		// 分布式锁释放成功
		if err2 == rpc.OK {
			log.Printf("%v 释放分布式锁成功\n", lk.me)
			break
		}
		// 释放锁时，出现未预料的情况
		if err2 == rpc.ErrVersion {
			log.Fatalf("出现未预料的情况: 串行的 Release 中调用 Put 发生 ErrVersion")
		}
		// 不知道分布式锁是否释放成功，需要额外判断
		if err2 == rpc.ErrMaybe {
			value, _, err := lk.ck.Get(lk.l)
			if err != rpc.OK {
				log.Fatalf("出现未预料的情况: Get 操作执行失败\n")
			}
			if value == lk.me {
				log.Printf("ErrMaybe 情况下, Put RPC 命令实际执行失败, %v 释放分布式锁失败, 循环重试\n", lk.me)
				continue
			} else {
				log.Printf("ErrMaybe 情况下, Put RPC 命令实际执行成功, %v 释放分布式锁成功\n", lk.me)
				break
			}
		}
	}
}
