package raft

import (
	"sort"
	"time"
)

type AppendEntriesArgs struct {
	Term         int   // 领导者任期：跟随者判断领导者的新旧
	LeaderId     int   // 领导者 id：用于将客户端请求重定向至领导者
	PrevLogIndex int   // 新日志的上一条日志的索引：用于一致性检查
	PrevLogTrem  int   // 新日志的上一条日志的任期：用于一致性检查
	Entries      []Log // 复制给跟随者的日志条目：对于心跳机制，此属性为空
	LeaderCommit int   // 领导者已提交的最高日志索引：用于同步跟随者的提交
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var logType logTopic
	if len(args.Entries) != 0 {
		logType = dLog
		Debugf(logType, "S%d <- S%d 收到追加日志请求", rf.me, args.LeaderId)
	} else {
		logType = dLog2
		Debugf(logType, "S%d <- S%d 收到心跳请求", rf.me, args.LeaderId)
	}

	// 1. 对比任期
	// 如果当前任期小于领导者的任期, 存在三种可能, 跟随者, 候选人, 领导者
	// 如果是跟随者，更新任期，重置选票
	// 如果是候选人，更新任期，重置选票，变为 Follower
	// 如果是领导者，更新任期，重置选票，变为 Follower
	if rf.currentTerm < args.Term {
		if rf.state != Follower {
			Debugf(logType, "S%d 任期过期, %v -> Follower", rf.me, rf.state)
			rf.state = Follower
		}
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	} else if rf.currentTerm > args.Term {
		// 如果跟随者的任期大于领导者，跟随者返回自身任期与 false，领导者收到响应后，会变为跟随者
		Debugf(logType, "S%d 判定 S%d 存在“过期领导者问题”", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		// 候选人发现正确的领导者，变为跟随者（可以去掉这一步吗？）
		if rf.state == Candidate {
			Debugf(logType, "S%d %v -> Follower", rf.me, rf.state)
			rf.state = Follower
		}
	}

	rf.lastHeartbeat = time.Now()

	// 一致性检查
	reply.ConflictIndex = -1 // 该变量用于判断是否存在一致性问题，跟随者与领导者都根据此变量判断
	reply.ConflictTerm = -1
	if args.PrevLogIndex < rf.lastIncludedIndex {
		Debugf(dError, "S%d 发生了完全未知的情况, 丢弃掉本次 RPC")
		reply.Term = args.Term
		reply.Success = false
		return
	}
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictIndex = rf.logLen()
	} else if prevLogTrem := rf.logTerm(args.PrevLogIndex); prevLogTrem != args.PrevLogTrem {
		// 遍历第一条不匹配的日志
		for i := rf.lastIncludedIndex; i < rf.logLen(); i++ {
			if rf.logTerm(i) == prevLogTrem {
				reply.ConflictIndex = i
				reply.Term = prevLogTrem
				break
			}
		}
	}
	if reply.ConflictIndex != -1 {
		Debugf(logType, "S%d 判定存在“一致性问题” {reply.ConflictIndex:%d}", rf.me, reply.ConflictIndex)
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
			// 如果新条目的索引越界，或新条目与本地条目任期不一致，则当前索引为冲突点，截断日志并追加新条目
			if newEntryIndex > rf.lastLogIndex() || rf.logTerm(newEntryIndex) != entry.Term {
				// 这里不会出现数组越界问题
				// 原因：根据一致性检查，必然存在 args.PrevLogIndex <= len(rf.logs) - 1。
				//      根据条目公式，newEntryIndex 可能的最大值为 len(rf.logs)，符合语法。
				rf.truncateLog(rf.lastIncludedIndex, newEntryIndex)
				rf.appendEntries(args.Entries[i:])
				Debugf(logType, "S%d 追加日志成功", rf.me)
				break
			} else {
				continue
			}
		}
	}

	// 正常情况
	Debugf(logType, "S%d -> S%d 追加日志成功", rf.me, args.LeaderId)
	// 易错点:
	// 要求 1: 论文图二要求 min 的第二个参数为 index of last new entry, 但是心跳请求压根就没有新条目。
	// 要求 2: 论文 5.3 节明确说明该操作 including heartbeats
	// 因此将 args.PrevLogIndex + len(args.Entries) 作为最后一个新条目的索引, 以便符合要求
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex + len(args.Entries)) // 将已提交索引与领导者同步
	}
	reply.Term = args.Term
	reply.Success = true

	rf.persist()
}

func (rf *Raft) sendEntries(server int, logType logTopic) {
	rf.mu.Lock()

	// 在释放锁又持有锁的间隔，nextIndex 可能再次被更改，因此需要再次检查
	needSnapshoting := rf.nextIndex[server] <= rf.lastIncludedIndex
	// if needSnapshoting && logType == dLog { // 必须是追加日志请求！否则把心跳也给跳过了，导致心跳被阻塞
	if needSnapshoting {
		Debugf(dInfo, "S%d -> S%d sendEntries 时发现需要发送快照", rf.me, server)
		rf.mu.Unlock()
		return
	}

	var entries []Log
	switch logType {
	case dLog:
	    Debugf(logType, "S%d 本次为追加日志请求", rf.me)
	    slice := rf.logEntries(rf.nextIndex[server], rf.logLen()) // 切片, 包含 logs 中从 nextIndex 到结束的所有元素
	    entries = make([]Log, len(slice))
	    copy(entries, slice)
	case dLog2:
	    Debugf(logType, "S%d 本次为心跳请求", rf.me)
	    entries = []Log{}
	default:
		Debugf(dError, "S%d 参数 logType %v 错误", rf.me, logType)
	    panic("参数 logType 错误")
	}

	currentTerm := rf.currentTerm
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.logTerm(prevLogIndex)
	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTrem:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	okChan := make(chan bool, 1)
	Debugf(logType, "S%d -> S%d 发送 AppendEntries RPC, {Term:%v, PrevLogIndex:%v, PrevLogTerm:%v, len(Entries):%v}", rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTrem, len(args.Entries))
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		okChan <- ok
	}()
	select {
	case ok := <-okChan:
		// 如果 RPC 正常返回（无论成功还是网络失败）
		if !ok {
			return
		}
	case <-time.After(100 * time.Millisecond):
		// 如果超时
		Debugf(logType, "S%d -> S%d AppendEntries RPC 超时 (100ms)", rf.me, server)
		return
	}

	// Debugf(logType, "S%d -> S%d 发送 AppendEntries RPC, {Term:%v, PrevLogIndex:%v, PrevLogTerm:%v, len(Entries):%v}", rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTrem, len(args.Entries))
	// ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	// if !ok {
	// 	return
	// }

	rf.mu.Lock()
	Debugf(logType, "S%d <- S%d 收到 AppendEntries RPC 响应, reply: {Success:%v, Term:%v, ConflictIndex:%v}", rf.me, server, reply.Success, reply.Term, reply.ConflictIndex)
	// 根据返回结果进行处理
	if currentTerm != rf.currentTerm {
		Debugf(logType, "S%d 判定响应来自旧任期, 丢弃", rf.me)
		rf.mu.Unlock()
		return
	}
	// 如果因任期问题失败，立即退位
	if !reply.Success && rf.currentTerm < reply.Term {
		Debugf(logType, "S%d 任期过期, Leader -> Follower", rf.me)
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}
	// 如果因不一致问题失败，nextIndex 变小重试
	if !reply.Success && reply.ConflictIndex != -1 {
		Debugf(logType, "S%d 判定存在一致性问题", rf.me)
		rf.nextIndex[server] = reply.ConflictIndex
		// 检查自己有没有对应任期，如果有的话，跳跃
		if reply.ConflictTerm != -1 {
			for i := rf.logLen(); i >= rf.lastIncludedIndex; i-- {
				if rf.logTerm(i) == reply.ConflictTerm {
					rf.nextIndex[server] = i + 1
					break
				}
			}
		}
		rf.mu.Unlock()
		return
	}
	if !reply.Success {
		Debugf(dError, "S%d 发生了完全未知的情况, 丢弃掉本次 RPC", rf.me)
		rf.mu.Unlock()
		return
	}
	// 如果成功
	if len(args.Entries) == 0 {
		// 想了半天，感觉对于心跳来说，下面的逻辑没什么意义。
		rf.mu.Unlock()
		return
	}
	// - 更新 matchIndex nextIndex
	rf.matchIndex[server] = prevLogIndex + len(args.Entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	Debugf(logType, "S%d AppendEntries RPC 成功, matchIndex=%d, nextIndex=%d", rf.me, rf.matchIndex[server], rf.nextIndex[server])
	// - 根据 matchIndex，判断是否更新 commitIndex
	matchIndexCopy := make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)
	sort.Ints(matchIndexCopy)                               // 升序排序
	newCommitIndex := matchIndexCopy[len(matchIndexCopy)/2] // 以降序过半数为准, 即为升序恰好没过半数的索引, 例如 1/3, 2/2, 3/5
	// 只允许提交当前任期的条目
	if newCommitIndex > rf.commitIndex && rf.logTerm(newCommitIndex) == rf.currentTerm {
		Debugf(logType, "S%d newCommitIndex=%d, rf.commitIndex=%d, 更新 rf.commitIndex", rf.me, newCommitIndex, rf.commitIndex)
		rf.commitIndex = newCommitIndex
	} else {
		Debugf(logType, "S%d newCommitIndex=%d, rf.commitIndex=%d, 无需更新", rf.me, newCommitIndex, rf.commitIndex)
	}
	rf.mu.Unlock()
}
