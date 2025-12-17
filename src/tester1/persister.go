package tester

//
// 为 Raft 和 kvraft 提供持久化支持
// 保存 Raft 持久状态（日志等）和 k/v 服务器快照
//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

// 模拟持久化存储。实际存储在内存中
type Persister struct {
	mu        sync.Mutex
	raftstate []byte // 存储 Raft 的持久化状态
	snapshot  []byte // 存储 kvserver 的快照数据
}

func MakePersister() *Persister {
	return &Persister{}
}

// 创建并返回输入字节切片的拷贝
func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig) // 该函数本身是浅拷贝
	return x
}

// 创建并返回 Persister 的副本（浅拷贝）
func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

// 返回 Raft 持久化状态的副本
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

// 返回 Raft 持久化状态的数据大小
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save 以单个原子操作，同时保存 Raft 状态和 K/V 快照，
// 有助于避免二者不同步。
//
// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
