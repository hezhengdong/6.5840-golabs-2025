package raft

import (
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debugf(dSnap, "S%d <- S%d 收到安装快照请求", rf.me, args.LeaderId)

	// 对比任期的逻辑应当封装起来，重复出现三次了
	// 领导者过期，直接丢弃
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		if rf.state != Follower {
			Debugf(dSnap, "S%d 任期过期, %v -> Follower", rf.me, rf.state)
			rf.state = Follower
		}
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// 索引过期，直接丢弃
	if args.LastIncludedIndex <= rf.commitIndex {
		Debugf(dSnap, "S%d 抛弃过期请求 args.LastIncludedIndex %v <= rf.commitIndex %v", rf.me, args.LastIncludedIndex, rf.commitIndex)
		reply.Term = -1
		return
	}

	rf.lastHeartbeat = time.Now()

	snapIndex := args.LastIncludedIndex
	if snapIndex > rf.lastLogIndex() || rf.logTerm(snapIndex) != args.LastIncludedTerm {
		Debugf(dSnap, "S%d 清空日志, 覆盖全部快照, 追加 lastIncludedIndex, 充当虚拟头节点", rf.me)
		rf.truncateLog(rf.lastIncludedIndex, rf.lastIncludedIndex) // 清空日志，保留最后一个元素
		rf.appendNewEntry(Log{
			Term: args.LastIncludedTerm,
			Command: nil,
		})
	} else if rf.logTerm(snapIndex) == args.LastIncludedTerm {
		Debugf(dSnap, "S%d 截断被快照覆盖的日志, 但保留 lastIncludedIndex, 充当虚拟头节点", rf.me)
		rf.truncateLog(snapIndex, rf.logLen())
	}

	// 添加快照信息
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data

	// 更新提交信息
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	Debugf(dSnap, "S%d <- S%d 处理成功", rf.me, args.LeaderId)
	reply.Term = args.Term

	rf.snapshotPending = true
	rf.persist()
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()

	// 在释放锁又持有锁的间隔，nextIndex 可能再次被更改，因此需要再次检查
	needSnapshoting := rf.nextIndex[server] <= rf.lastIncludedIndex
	if !needSnapshoting {
		Debugf(dInfo, "S%d sendInstallSnapshot 时发现不需要发送了", rf.me)
		rf.mu.Unlock()
		return
	}

	currentTerm := rf.currentTerm
	args := InstallSnapshotArgs {
		Term: currentTerm,
		LeaderId: rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.lastIncludedTerm,
		Data: rf.snapshot,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	okChan := make(chan bool, 1)
	Debugf(dSnap, "S%d -> S%d 发送 InstallSnapshot RPC args: {term: %v, lastIncludedIndex: %v, lastIncludedTerm: %v}", rf.me, server, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
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
		Debugf(dSnap, "S%d -> S%d InstallSnapshot RPC 超时 (100ms)", rf.me, server)
		return
	}

	rf.mu.Lock()
	if currentTerm != rf.currentTerm {
		Debugf(dSnap, "S%d 过去任期的响应, 丢弃", rf.me)
		rf.mu.Unlock()
		return
	}
	if reply.Term > currentTerm {
		Debugf(dSnap, "S%d 任期过期, Leader -> Follower", rf.me)
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}
	if reply.Term == -1 {
		Debugf(dSnap, "S%d 没必要的请求, 被跟随者丢弃", rf.me)
		// 代码执行到这里，意味着从节点不需要主节点的 InstallSnapshot，应当将对应的 nextIndex 重置为最大值
		rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.mu.Unlock()
		return
	}
	// 快照发送成功，更新 matchIndex 和 nextIndex
    rf.matchIndex[server] = args.LastIncludedIndex
    rf.nextIndex[server] = args.LastIncludedIndex + 1
	Debugf(dSnap, "S%d 接收到 InstallSnapshot, ", rf.me)
	rf.mu.Unlock()
}
