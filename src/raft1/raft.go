package raft

// 文件定义了 raft 必须向服务器公开的接口
//
// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic" // 原子操作
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// 实现单个 Raft 对等节点的 Go 对象
type Raft struct {
	mu        sync.Mutex          // 用于保护对该对等节点状态的共享访问的锁 Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // 所有对等节点的 RPC 网络访问地址 RPC end points of all peers
	persister *tester.Persister   // 用于存储此对等节点持久化状态的对象 Object to hold this peer's persisted state
	me        int                 // 该对等节点在 peers[] 中的索引 this peer's index into peers[]
	dead      int32               // 由 Kill() 设置 set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 服务器当前所处的状态
	state           State
	// 所有服务器上的持久状态
	currentTerm     int   // 服务器所知的最新任期
	votedFor        int   // 服务器的投票人选
	logs            []Log // 日志条目
	// 所有服务器上的不稳定状态
	commitIndex     int   // 服务器上已提交的最高索引
	lastApplied     int   // 服务器上已被应用的最高索引
	// 领导者上的不稳定状态
	nextIndex       []int // 对于每个服务器，要发送到该服务器的下一个日志条目的索引
	matchIndex      []int // 对于每个服务器，已复制的最高索引
	// 选举定时器（自己加的）
	lastHeartbeat   time.Time // 最后一次心跳时间
	electionTimeout time.Duration // 真正的超时阈值
	// 条件变量，用于阻塞 goroutine
	cond            *sync.Cond
	// 快照相关
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	applyCh chan raftapi.ApplyMsg
	snapshotPending   bool
}

const (
	// 常量, 只会被访问
	HeartbeatInterval   time.Duration = 100 * time.Millisecond // 心跳间隔
	BaseElectionTimeout time.Duration = 750 * time.Millisecond // 基础超时阈值
	RandomCount = 750
)

type State string

const (
	Leader    State = "Leader"
	Candidate State = "Candidate"
	Follower  State = "Follower"
)

type Log struct {
	Term    int
	Command interface{}
}

// 返回 currentTerm 以及服务器是否认为自己是 leader
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = rf.currentTerm
	var isleader bool = rf.state == Leader
	return term, isleader
}

// 将 Raft 的持久化状态保存到稳定存储中，
// 以便在崩溃和重启后可以检索。
// 有关应当持久化的内容的描述，将参阅论文的图 2。
// 在实现快照之前，你应该将 nil 作为第二个参数传递给 persister.Save().
// 在实现快照之后，传递当前快照（如果还没有快照，则为 nil）。
func (rf *Raft) persist() {
	// 这里写入数据，把持久化的数据转换为字节，存储进 Persister 对象中
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
	Debugf(dPersist, "S%d 持久化 {currentTerm:%d, votedFor:%d, len(logs):%d, len(snapshot):%v}", rf.me, rf.currentTerm, rf.votedFor, rf.logLen(), len(rf.snapshot))
}

// 恢复到之前持久化的状态
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm       int
	var votedFor          int
	var logs              []Log
	var lastIncludedIndex int
	var lastIncludedTerm  int
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&logs) != nil ||
	   d.Decode(&lastIncludedIndex) != nil ||
	   d.Decode(&lastIncludedTerm) != nil {
		panic("解码失败")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		// 初始化 commitIndex 和 lastApplied 为快照索引
        rf.commitIndex = lastIncludedIndex
        rf.lastApplied = lastIncludedIndex
		// 还有读取持久化存储的快照
		rf.snapshot = rf.persister.ReadSnapshot()
	}
	// 重置时间，防止不必要的选举
	rf.lastHeartbeat = time.Now()

	Debugf(dPersist, "S%d 读取持久状态 {term:%d, votedFor:%d, len(logs):%d, lastIncludedIndex:%v, lastIncludedTerm:%v, len(rf.snapshot):%v}", rf.me, currentTerm, votedFor, len(logs), lastIncludedIndex, lastIncludedTerm, len(rf.snapshot))
}

// Raft 持久化日志中有多少字节？
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 服务表示它已创建一个快照，其中包含截至并包括 index 的所有信息。
// 这意味着服务不再需要通过（并包含）该索引的日志。
// Raft 现在应该尽可能地修剪其日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	if rf.killed() {
        return
    }
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 抛弃过期请求
	if index <= rf.lastIncludedIndex {
		return
	}
	// 先截断日志
	rf.truncateLog(index, rf.logLen())
	// 后保存新数据（二者顺序一定不能错！截断日志操作需要依赖旧的 lastIncludedIndex！）
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logTerm(index)
	rf.snapshot = snapshot
	Debugf(dSnap, "S%d 创建快照 {lastIncludedIndex: %v, lastIncludedTerm: %v, len(snapshot): %v}", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.snapshot))
	// 保存快照
	rf.persist()
}

// 该函数用于接收客户端的命令
//
// 规则如下：
// 使用 Raft 的服务（例如 k/v 服务器）希望开始就要附加到 Raft 日志的下一个命令达成一致。
// 如果该服务器不是 leader，则返回 false。
// 否则启动协议并立即返回。不能保证此命令会被提交到 Raft 日志，
// 因为 leader 可能失败或输掉选举。即使 Raft 实例已被杀死，
// 此函数也应该正常返回。
//
// 第一个返回值是命令在被提交时将出现的索引。
// 第二个返回值是当前任期。
// 第三个返回值是如果该服务器认为自己是 leader，则为 true。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return 0, 0, false
	}

	// 追加日志
	rf.appendNewEntry(Log{
		Term: rf.currentTerm,
		Command: command,
	})
	Debugf(dClient, "S%d 接收并追加 log: {Term:%v, Command:%v}", rf.me, rf.currentTerm, command)
	rf.persist()

	// 更新自身状态
	rf.nextIndex[rf.me] = rf.lastLogIndex() + 1
	rf.matchIndex[rf.me] = rf.lastLogIndex()

	// 返回值
	index := rf.lastLogIndex()
	term := rf.currentTerm
	return index, term, true
}

// 测试器不会在每次测试后停止 Raft 创建的 goroutine，
// 但它会调用 Kill() 方法。你的代码可以使用 killed() 来检查是否调用了 Kill()。
// 使用 atomic 避免了对锁的需求。
//
// 问题是长时间运行的 goroutine 会占用内存并可能消耗 CPU 时间，
// 可能导致后续测试失败并生成令人困惑的调试输出。
// 任何具有长时间运行循环的 goroutine 都应该调用 killed() 来检查是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		if (rf.state != Leader && time.Since(rf.lastHeartbeat) > rf.electionTimeout) {
			Debugf(dVote, "S%d 检测到选举超时 %v, 启动选举", rf.me, rf.electionTimeout)
			go rf.startElection()
			rf.lastHeartbeat = time.Now() // 重置选举超时时间
			rf.electionTimeout = BaseElectionTimeout + time.Duration(rand.Int63() % RandomCount) * time.Millisecond // 为选举超时时间赋予新的随机值
			rf.mu.Unlock()
			continue
		}

		if (rf.state == Leader) {
			rf.mu.Unlock()
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				go rf.sendEntries(server, dLog2)
			}
			time.Sleep(HeartbeatInterval)
			continue
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) replicator(server int) {
	for !rf.killed() {		
		rf.mu.Lock()

		for rf.state != Leader {
			Debugf(dInfo, "S%d 不是 Leader, 挂起 S%d replicator", rf.me, server)
			rf.cond.Wait()
		}

		// 如果 nextIndex <= 快照中的最后一个索引，因为条目已被快照，Leader 无法提供，因此需要 InstallSnapshot
		needSnapshoting := rf.nextIndex[server] <= rf.lastIncludedIndex // 这里会存在活锁问题
		needReplicating := rf.logLen() > rf.nextIndex[server]
		Debugf(dInfo, "S%d -> S%d {needSnapshoting: %v, needReplication: %v}", rf.me, server, needSnapshoting, needReplicating)
		if !needSnapshoting && !needReplicating {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if needSnapshoting { // 需要发送快照
			rf.mu.Unlock()
			rf.sendInstallSnapshot(server)
			continue
		} else { // 不需要发送快照
			rf.mu.Unlock()
			rf.sendEntries(server, dLog)
			continue
		}
	}
}


func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.snapshotPending {
            msg := raftapi.ApplyMsg{
                SnapshotValid: true,
                Snapshot:      rf.snapshot,
                SnapshotTerm:  rf.lastIncludedTerm,
                SnapshotIndex: rf.lastIncludedIndex,
            }
            rf.snapshotPending = false
            rf.mu.Unlock()
            rf.applyCh <- msg
            Debugf(dCommit, "S%d 成功应用快照 {lastIncludedIndex: %v, lastIncludedTerm: %v}", rf.me, msg.SnapshotIndex, msg.SnapshotTerm)
            continue
        }
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command: rf.logCommand(rf.lastApplied),
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			Debugf(dCommit, "S%d 成功应用条目 {CommandIndex: %d, Command: %v}", rf.me, msg.CommandIndex, msg.Command)
			rf.applyCh <- msg
			continue
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// 服务或测试器想要创建一个 Raft 服务器。
// 所有 Raft 服务器（包括这个）的端口都在 peers[] 中。
// 该服务器的端口是 peers[me]。所有服务器的 peers[] 数组具有相同的顺序。
// persister 是该服务器保存其持久化状态的地方，
// 并且最初还保存最近保存的状态（如果有）。
// applyCh 是一个通道，测试器或服务期望 Raft 在该通道上发送 ApplyMsg 消息。
// Make() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers: peers,
		persister: persister,
		me: me,
		state: Follower,
		// 所有服务器上的稳定状态
		currentTerm: 0,
		votedFor: -1,
		logs: make([]Log, 1),
		// 所有服务器上的稳定状态
		commitIndex: 0,
		lastApplied: 0,
		// 领导者上的不稳定状态
		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		// 其他
		lastHeartbeat: time.Now().Add(-BaseElectionTimeout),
		electionTimeout: BaseElectionTimeout + time.Duration(rand.Int63() % RandomCount) * time.Millisecond,
		lastIncludedIndex: 0,
		lastIncludedTerm: 0,
		snapshot: nil,
		applyCh: applyCh,
	}

	// 初始化虚拟日志，避免复杂的边界条件。且论文图 2 表示索引从 1 开始接收
	rf.logs[0] = Log{
		Term: rf.lastIncludedTerm,
		Command: nil,
	}

	// 初始化 nextIndex[]
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	// 初始化条件变量
	rf.cond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	Debugf(dInfo, "S%d 初始化", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()
	// 为每个 peer 启动一个 replicator
	// replicator 定时将 logs[nextIndex] ~ logs[-1] 复制到对应的 peer 上
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.replicator(server)
	}
	// 应用已提交的日志
	go rf.applier()

	return rf
}
