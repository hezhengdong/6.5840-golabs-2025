package lock

import (
	"log"
	"time"

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
}

var unlocked string = "unlocked"
var locked string = "locked"

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
	lk := &Lock{ck: ck, l: l}
	// You may add code here
	return lk
}

// 思路：服务端需要维护一个标记来判断分布式锁的持有与释放
// Acquire：
// - 如果 value == unlocked，尝试 Put，如果发生 ErrVersion，获取锁失败，循环重试
// - 如果 value == locked，获取锁失败，循环重试
// Release：
// - 因为 Acquire 阻塞，同时只会有一个客户端执行该操作，不会有并发问题，直接修改键值即可

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.l)

		// 如果整个系统第一次请求分布式锁，构建最初的键值对
		if err == rpc.ErrNoKey {
			// 此处可能会有并发问题，多个进程执行这行代码。
			// 但是根据服务端的版本号机制，只会有一个客户端执行成功，此处并发不影响结果。
			lk.ck.Put(lk.l, unlocked, 0)
			continue
		}

		// 如果分布式锁已被其他客户端持有，循环尝试
		if value == locked {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 如果分布式锁未被其他客户端持有，尝试持有锁。
		// 这行代码存在并发问题，多个客户端同时执行，
		// 版本号机制确保只有一个客户端会成功执行，根据返回值判断是否成功获取锁
		err2 := lk.ck.Put(lk.l, locked, version)

		// 如果成功持有锁，结束方法
		if err2 == rpc.OK {
			break
		}

		// 如果未成功抢占锁，循环尝试
		if err2 == rpc.ErrVersion {
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	// 根据分布式锁的逻辑，Release 方法一定会被串行执行，不存在并发问题。错误检测预防疏漏
	_, version, err := lk.ck.Get(lk.l)
	if err != rpc.OK {
		log.Printf("释放分布式锁失败")
	}
	err2 := lk.ck.Put(lk.l, unlocked, version)
	if err2 != rpc.OK {
		log.Printf("释放分布式锁失败")
	}
}
