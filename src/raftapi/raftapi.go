package raftapi

// The Raft interface
// 该接口定义了 Raft 的核心方法
type Raft interface {
	// Start agreement on a new log entry, and return the log index
	// for that entry, the term, and whether the peer is the leader.
	Start(command interface{}) (int, int, bool)

	// Ask a Raft for its current term, and whether it thinks it is
	// leader
	GetState() (int, bool)

	// For Snaphots (3D)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int

	// For the tester to indicate to your code that is should cleanup
	// any long-running go routines.
	Kill()
}

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the server (or
// tester), via the applyCh passed to Make(). Set CommandValid to true
// to indicate that the ApplyMsg contains a newly committed log entry.
//
// In Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool        // 为 true 表示包含新提交的日志条目
	Command      interface{} // 提交的命令内容
	CommandIndex int         // 命令在日志中的索引位置

	SnapshotValid bool   // 为 true 表示包含快照数据
	Snapshot      []byte // 快照数据字节数组
	SnapshotTerm  int    // 快照对应的任期
	SnapshotIndex int    // 快照对应的日志索引
}
