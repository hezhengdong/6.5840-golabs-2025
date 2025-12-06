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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic" // 原子操作
	"time"

	//	"6.5840/labgob"
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
	lastHeartbeat       time.Time // 最后一次心跳时间
	electionTimeout      time.Duration // 真正的超时阈值
	// 条件变量，用于阻塞 goroutine
	cond *sync.Cond
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
//
// return currentTerm and whether this server
// believes it is the leader.
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
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// 恢复到之前持久化的状态
//
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// Raft 持久化日志中有多少字节？
//
// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 服务表示它已创建一个快照，其中包含截至并包括 index 的所有信息。
// 这意味着服务不再需要通过（并包含）该索引的日志。
// Raft 现在应该尽可能地修剪其日志。
//
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// RequestVote RPC 参数结构示例。
// 字段名必须以大写字母开头！
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

// RequestVote RPC 回复结构示例。
//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC 处理程序示例。
// 客户端填充参数 -> 服务端
// 服务端填充返回值 -> 客户端
//
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debugf(dVote, "S%d <- S%d: 收到投票请求", rf.me, args.CandidateId)

	// 1. 对比投票者与候选人的任期
	// 如果投票者任期更大，拒绝投票
	if rf.currentTerm > args.CandidateTerm {
		Debugf(dVote, "S%d 判定存在“候选人过期问题”, S%d.Term = %d, S%d.Term = %d", rf.me, rf.me, rf.currentTerm, args.CandidateId, args.CandidateTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 如果当前任期小于领导者的任期, 存在三种可能, 跟随者, 候选人, 领导者
	// 如果是跟随者，更新任期，重置选票
	// 如果是候选人，更新任期，重置选票，变为 Follower
	// 如果是领导者，更新任期，重置选票，变为 Follower
	if rf.currentTerm < args.CandidateTerm {
		if rf.state != Follower {
			Debugf(dLog, "S%d 任期过期, %v -> Follower", rf.me, rf.state)
			rf.state = Follower
		}
		rf.currentTerm = args.CandidateTerm
		rf.votedFor = -1
	}

	// 2. 检查投票者是否已经投过票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		Debugf(dVote, "S%d 判定存在“已投票问题”, voteFor = %d", rf.me, rf.votedFor)
		reply.Term = args.CandidateTerm
		reply.VoteGranted = false
		return
	}

	// 3. 对比二者日志，哪个更加完善（5.4.1 选举限制）
	// 如果投票者的日志比候选人更加完善，则拒绝投票
	lastLogTerm := rf.logs[rf.commitIndex].Term
	lastLogIndex := rf.commitIndex
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		Debugf(dVote, "S%d 判定存在“候选人条件不足问题”", rf.me)
		Debugf(dVote, "S%d 本机: lastLogTerm = %d, lastLogIndex = %d", rf.me, lastLogTerm, lastLogIndex)
		Debugf(dVote, "S%d 候选人: lastLogTerm = %d, lastLogIndex = %d", rf.me, args.CandidateTerm, args.LastLogIndex)
		reply.Term = args.CandidateTerm
		reply.VoteGranted = false
		return
	}

	// 只有以上三个条件都满足，投票者才为该候选人投票
	Debugf(dVote, "S%d -> S%d: 为候选人投票, Term=%d", rf.me, args.CandidateId, rf.currentTerm)
	rf.lastHeartbeat = time.Now()
	rf.votedFor = args.CandidateId
	reply.Term = args.CandidateTerm
	reply.VoteGranted = true
}

// 向服务器发送 RequestVote RPC 的示例代码。
// server 是目标服务器在 rf.peers[] 中的索引。
// 期望 RPC 参数在 args 中。
// 用 RPC 回复填充 *reply，因此调用者应该传递 &reply。
// 传递给 Call() 的 args 和 reply 的类型必须与处理程序函数中声明的参数类型相同
//（包括它们是否为指针）。
//
// labrpc 包模拟有损网络，其中服务器可能无法访问，
// 并且请求和回复可能会丢失。
// Call() 发送请求并等待回复。如果回复在超时间隔内到达，
// Call() 返回 true；否则 Call() 返回 false。
// 因此 Call() 可能不会立即返回。
// 错误的返回可能是由于服务器死机、无法访问的活动服务器、
// 丢失的请求或丢失的回复造成的。
//
// Call() 保证返回（可能在延迟之后）*除非*服务器端的处理程序函数不返回。
// 因此无需在 Call() 周围实现你自己的超时。
//
// 查看 ../labrpc/labrpc.go 中的注释以获取更多详细信息。
//
// 如果你在使 RPC 工作时遇到问题，请检查你是否已将通过 RPC 传递的结构中的
// 所有字段名大写，并且调用者使用 & 传递回复结构的地址，而不是结构本身。
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int   // 领导者任期：跟随者判断领导者的新旧
	LeaderId     int   // 领导者 id：用于将客户端请求重定向至领导者
	PrevLogIndex int   // 新日志的上一条日志的索引：用于一致性检查
	PrevLogTrem  int   // 新日志的上一条日志的任期：用于一致性检查
	Entries      []Log // 复制给跟随者的日志条目：对于心跳机制，此属性为空
	LeaderCommit int   // 领导者已提交的最高日志索引：用于同步跟随者的提交
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) == 0 {
		Debugf(dLog2, "S%d <- S%d 收到追加日志请求", rf.me, args.LeaderId)
	} else {
		Debugf(dLog, "S%d <- S%d: 收到心跳请求", rf.me, args.LeaderId)
	}

	// 1. 对比任期
	// 如果当前任期小于领导者的任期, 存在三种可能, 跟随者, 候选人, 领导者
	// 如果是跟随者，更新任期，重置选票
	// 如果是候选人，更新任期，重置选票，变为 Follower
	// 如果是领导者，更新任期，重置选票，变为 Follower
	if rf.currentTerm < args.Term {
		if rf.state != Follower {
			Debugf(dLog, "S%d 任期过期, %v -> Follower", rf.me, rf.state)
			rf.state = Follower
		}
		rf.currentTerm = args.Term
		rf.votedFor = -1
	} else if rf.currentTerm > args.Term {
		// 如果跟随者的任期大于领导者，跟随者返回自身任期与 false，领导者收到响应后，会变为跟随者
		Debugf(dLog, "S%d 判定 S%d 存在“过期领导者问题”", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		// 候选人发现正确的领导者，变为跟随者
		if rf.state == Candidate {
			rf.state = Follower
		}
	}

	rf.lastHeartbeat = time.Now()

	// 2. 一致性检查，检查失败，返回 false。领导者收到 false 后，nextIndex-- 重试
	if (len(rf.logs) - 1) < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTrem {
		Debugf(dLog, "S%d 判定存在“一致性问题”", rf.me)
		reply.Term = args.Term
		reply.Success = false
		return
	}

	// 如果是追加日志请求
	if len(args.Entries) != 0 {
		// 遍历领导者发来的追加条目
		for i, entry := range args.Entries {
			// 新条目在当前服务器日志中，应当所处的索引
			newEntryIndex := args.PrevLogIndex + 1 + i
			// 如果新条目的索引越界，或新条目与本地条目不一致，则当前索引为冲突点，截断日志并追加新条目
			if newEntryIndex > len(rf.logs) - 1 || rf.logs[newEntryIndex].Term != entry.Term {
				// 这里不会出现数组越界问题
				// 原因：根据一致性检查，必然存在 args.PrevLogIndex <= len(rf.logs) - 1。
				//      根据条目公式，newEntryIndex 可能的最大值为 len(rf.logs)，符合语法。
				rf.logs = rf.logs[:newEntryIndex]
				rf.logs = append(rf.logs, args.Entries[i:]...)
				Debugf(dLog2, "S%d 追加日志成功", rf.me)
				break
			} else {
				continue
			}
		}
	} else {
		Debugf(dLog, "S%d -> S%d: 响应心跳请求", rf.me, args.LeaderId)
	}

	// 正常情况
	Debugf(dLog, "S%d -> S%d: {args.LeaderCommit=%d,rf.commitIndex=%d}", rf.me, args.LeaderId, args.LeaderCommit, rf.commitIndex)
	// 易错点: 
	// 要求 1: 论文图二要求 min 的第二个参数为 index of last new entry, 但是心跳请求压根就没有新条目。
	// 要求 2: 论文 5.3 节明确说明该操作 including heartbeats
	// 因此将 args.PrevLogIndex + len(args.Entries) 作为最后一个新条目的索引, 以便符合要求
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex + len(args.Entries)) // 将已提交索引与领导者同步
	}
	reply.Term = args.Term
	reply.Success = true
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
	log := Log{
		Term: rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, log)
	Debugf(dLog2, "S%d 接收并追加 log{Term=%v,Command=%v}", rf.me, log.Term, log.Command)

	// 更新相关状态
	rf.nextIndex[rf.me] = len(rf.logs)
	rf.matchIndex[rf.me] = len(rf.logs) - 1

	// 返回值
	index := len(rf.logs) - 1
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

// 选举线程
func (rf *Raft) startElection() {
	// 局部变量
	var grantedVotes int = 1 // 统计收到的票数, 并为自己投票
	results := make(chan bool, len(rf.peers) - 1)  // 无法获取异步执行函数返回值, 所以通过 channel 传递信息
	var wg sync.WaitGroup

	rf.mu.Lock()
	rf.state = Candidate // 修改状态
	rf.currentTerm++     // 任期+1
	rf.votedFor = rf.me  // 重置选票, 为自己投票
	Debugf(dVote, "S%d %v -> Candidate, Term=%d, 为自己投票", rf.me, rf.state, rf.currentTerm)

	// 1. 在持有锁时, 获取任期副本
	currentTerm := rf.currentTerm

	// 发送 RPC 请求
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		args := RequestVoteArgs{
			CandidateTerm: currentTerm,
			CandidateId: rf.me,
			LastLogIndex: rf.commitIndex,
			LastLogTerm: rf.logs[rf.commitIndex].Term,
		}
		reply := RequestVoteReply{}

		// 异步发送
		wg.Add(1)
		go func(server int) {
			defer wg.Done()

			// 2. 发送 RPC 请求
			Debugf(dVote, "S%d -> S%d 发起 RequestVote RPC", rf.me, server)
			ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			for i := 0; !ok && i < 5; i++ {
				time.Sleep(20 * time.Millisecond)
				ok = rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			}
			if !ok {
				results <- false
				return
			}

			// 3. 收到 RPC 响应
			rf.mu.Lock()
			defer rf.mu.Unlock()
			Debugf(dVote, "S%d <- S%d 收到 RequestVote RPC 响应", rf.me, server)
			if currentTerm != rf.currentTerm {
				Debugf(dVote, "S%d 判定响应来自旧任期, 丢弃", rf.me)
				results <- false
				return
			}
			// 拒绝投票
			if !reply.VoteGranted {
				if rf.currentTerm < reply.Term {
					Debugf(dVote, "S%d 任期过期, Candidate -> Follower", rf.me)
					rf.state = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				}
				results <- false
			} else {
				// 获得投票
				Debugf(dVote, "S%d <- S%d 投票成功, Term=%d", rf.me, server, rf.currentTerm)
				results <- true
			}
		}(server)
	}
	rf.mu.Unlock()

	go func() {
		wg.Wait()
		close(results)
	}()

	for success := range results {
		rf.mu.Lock()

		if success {
			grantedVotes++
		}

		if rf.state != Candidate || currentTerm != rf.currentTerm {
            Debugf(dVote, "S%d 选举终止", rf.me)
			rf.mu.Unlock()
            return
        }

		if grantedVotes >= (len(rf.peers) / 2 + 1) && rf.state == Candidate && currentTerm == rf.currentTerm {
			Debugf(dVote, "S%d 获得 %d/%d 票, Candidate -> Leader", rf.me, grantedVotes, len(rf.peers))
			rf.state = Leader // 修改状态，任期不变，选票不变
			// 初始化 nextIndex 与 matchIndex
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs)
			}
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
			}
			rf.cond.Broadcast() // 唤醒线程
			rf.mu.Unlock()
		    return
		}

		rf.mu.Unlock()
	}

	Debugf(dVote, "S%d 选举失败, 获得 %d/%d 票", rf.me, grantedVotes, len(rf.peers))
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server := range rf.peers {
		if server == rf.me { // 跳过自己
			continue
		}

		// 1. 获取任期副本 (Raft Locking Advice 规则 5)
		currentTerm := rf.currentTerm

		prevLogIndex := rf.nextIndex[server] - 1
		prevLogTerm := rf.logs[prevLogIndex].Term
		args := AppendEntriesArgs{
			Term: currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: prevLogIndex, // 这两个参数用于寻找领导者与跟随者间同步的日志位置
			PrevLogTrem: prevLogTerm,   // 即使在心跳机制中，这两个属性也能发挥作用
			Entries: []Log{},
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}

		go func(server int) {

			// 2. 发送 RPC 请求
			// Debugf(dLog, "S%d -> S%d 发起 AppendEntries RPC", rf.me, server)
			ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			// 如果 RPC 响应失败, 循环重试 (只循环 5 次, 避免无限循环, 进程无法终止)
			for i := 0; !ok && i < 5; i++ {
				time.Sleep(20 * time.Millisecond)
				ok = rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			}
			// 如果 RPC 重试失败, 丢弃回复并返回
			if !ok {
				return
			}

			// 3. 收到 RPC 响应
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// Debugf(dLog, "S%d <- S%d 收到 AppendEntries RPC 响应", rf.me, server)
			// 如果 RPC 执行过程中, 任期发生变化, 丢弃回复并返回 (Students' Guide to Raft 任期混淆章节)
			if currentTerm != rf.currentTerm {
				Debugf(dLog, "S%d 判定响应来自旧任期, 丢弃", rf.me)
				return
			}
			// 如果自身任期小于回复任期, 将状态由领导者修改为跟随者
			if !reply.Success && rf.currentTerm < reply.Term {
				Debugf(dLog, "S%d 任期过期, Leader -> Follower", rf.me)
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				return
			}
			// 如果依旧失败, 说明存在一致性问题
			if !reply.Success {
				Debugf(dLog, "S%d 判定存在一致性问题", rf.me)
				rf.nextIndex[server]--
			}
		}(server)
	}
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
			go rf.heartbeat()
			rf.mu.Unlock()
			time.Sleep(HeartbeatInterval)
			continue
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond) // AI 说 10ms 对现代 CPU 来说负载很小
	}
}

func (rf *Raft) replicator(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		for (rf.state != Leader) {
			Debugf(dLog2, "S%d 不是 Leader, 挂起 replicator", rf.me)
			rf.cond.Wait()
		}

		// 检查 nextIndex 与 len(rf.logs)，如果后者大于前者，且最新条目的日志等于当前条目，说明又有新的日志需要复制
		if (!(len(rf.logs) > rf.nextIndex[server] && rf.logs[len(rf.logs) - 1].Term == rf.currentTerm)) {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// 发送区间 [nextIndex, (len(rf.logs) - 1)] 内的所有数据（直接是切片）
		currentTerm := rf.currentTerm
		prevLogIndex := rf.nextIndex[server] - 1
		prevLogTerm := rf.logs[prevLogIndex].Term
		args := AppendEntriesArgs{
			Term: currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTrem: prevLogTerm,
			Entries: rf.logs[rf.nextIndex[server]:], // 切片, 包含 logs 中从 nextIndex 到结束的所有元素
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()

		Debugf(dLog2, "S%d -> S%d, 发起追加日志 RPC, term=%v, prevLogIndex=%v, prevLogTerm=%v, len(entries)=%v", rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTrem, len(args.Entries))
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		for !ok {
			time.Sleep(10 * time.Millisecond)
			ok = rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		}

		rf.mu.Lock()
		Debugf(dLog2, "S%d <- S%d, 收到追加日志 RPC 响应, reply.success=%v, reply.term=%v", rf.me, server, reply.Success, reply.Term)
		// 根据返回结果进行处理
		if currentTerm != rf.currentTerm {
			Debugf(dLog2, "S%d 判定响应来自旧任期, 丢弃", rf.me)
			rf.mu.Unlock()
			continue
		}
		// 如果因任期问题失败，立即退位
		if !reply.Success && rf.currentTerm < reply.Term {
			Debugf(dLog2, "S%d 任期过期, Leader -> Follower", rf.me)
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.mu.Unlock()
			continue
		}
		// 如果因不一致问题失败，nextIndex--
		if !reply.Success {
			Debugf(dLog2, "S%d 判定存在一致性问题", rf.me)
			rf.nextIndex[server]--
			rf.mu.Unlock()
			continue
		}
		// 如果成功
		// - 更新 matchIndex nextIndex
		rf.matchIndex[server] = prevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		Debugf(dLog2, "S%d 判定追加日志成功! matchIndex=%d, nextIndex=%d", rf.me, rf.matchIndex[server], rf.nextIndex[server])
		// - 根据 matchIndex，判断是否更新 commitIndex
		matchIndexCopy := make([]int, len(rf.matchIndex))
        copy(matchIndexCopy, rf.matchIndex)
		sort.Ints(matchIndexCopy) // 升序排序
		newCommitIndex := matchIndexCopy[len(matchIndexCopy) / 2] // 以降序过半数为准, 即为升序恰好没过半数的索引, 例如 1/3, 2/2, 3/5
		if newCommitIndex > rf.commitIndex {
			Debugf(dLog2, "S%d newCommitIndex=%d, rf.commitIndex=%d, 更新 rf.commitIndex", rf.me, newCommitIndex, rf.commitIndex)
			rf.commitIndex = newCommitIndex
		} else {
			Debugf(dLog2, "S%d newCommitIndex=%d, rf.commitIndex=%d, 无需更新", rf.me, newCommitIndex, rf.commitIndex)
		}
		rf.mu.Unlock()
	}
}

// 为每个 peer 启动一个 replicator
// replicator 定时将 logs[nextIndex] ~ logs[-1] 复制到对应的 peer 上
// 当 rf.state != Leader 时，挂起线程
func (rf *Raft) startReplicators() {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.replicator(server)
	}
	Debugf(dLog2, "S%d %d 个 replicator 线程启动完成", rf.me, len(rf.peers))
}

func (rf *Raft) applier(applyCh chan raftapi.ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command: rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			Debugf(dLog2, "S%d 成功应用条目 %d", rf.me, rf.lastApplied)
			rf.mu.Unlock()
			applyCh <- msg
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
		// 定时器
		lastHeartbeat: time.Now().Add(-BaseElectionTimeout),
		electionTimeout: BaseElectionTimeout + time.Duration(rand.Int63() % RandomCount) * time.Millisecond,
	}

	// 初始化虚拟日志，避免复杂的边界条件。且论文图 2 表示索引从 1 开始接收
	rf.logs[0] = Log{
		Term: 0,
		Command: "initial",
	}

	// 初始化 nextIndex[]
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	// 初始化条件变量
	rf.cond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// 启动复制线程
	go rf.startReplicators()
	// 应用已提交的日志
	go rf.applier(applyCh)


	return rf
}
