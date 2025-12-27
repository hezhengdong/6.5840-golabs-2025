package raft

import (
	"sync"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debugf(dVote, "S%d <- S%d 接收 RequestVote RPC, args: {%v}", rf.me, args.CandidateId, args)

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
		rf.persist()
	}

	// 2. 检查投票者是否已经投过票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		Debugf(dVote, "S%d 判定存在“已投票问题”, votedFor = %d", rf.me, rf.votedFor)
		reply.Term = args.CandidateTerm
		reply.VoteGranted = false
		return
	}

	// 3. 对比二者日志，哪个更加完善（5.4.1 选举限制）
	// 如果投票者的日志比候选人更加完善，则拒绝投票
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.logTerm(lastLogIndex)
	Debugf(dVote, "S%d 本机: lastLogTerm = %d, lastLogIndex = %d", rf.me, lastLogTerm, lastLogIndex)
	Debugf(dVote, "S%d 候选人: lastLogTerm = %d, lastLogIndex = %d", rf.me, args.CandidateTerm, args.LastLogIndex)
	candidateOutdated := lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex)
	if candidateOutdated {
		Debugf(dVote, "S%d 判定存在“候选人条件不足问题”", rf.me)
		reply.Term = args.CandidateTerm
		reply.VoteGranted = false
		return
	}

	// 只有以上三个条件都满足，投票者才为该候选人投票
	Debugf(dVote, "S%d -> S%d 为候选人投票, reply: {%v}", rf.me, args.CandidateId, reply)
	rf.lastHeartbeat = time.Now()
	rf.votedFor = args.CandidateId
	rf.persist()
	reply.Term = args.CandidateTerm
	reply.VoteGranted = true
}

// 选举线程
func (rf *Raft) startElection() {
	// 局部变量
	var grantedVotes int = 1 // 统计收到的票数, 并为自己投票
	results := make(chan bool, len(rf.peers) - 1)  // 无法获取异步执行函数返回值, 所以通过 channel 传递信息
	var wg sync.WaitGroup

	rf.mu.Lock()
	rf.currentTerm++     // 任期+1
	Debugf(dVote, "S%d %v -> Candidate, Term=%d, 为自己投票", rf.me, rf.state, rf.currentTerm)
	rf.state = Candidate // 修改状态
	rf.votedFor = rf.me  // 重置选票, 为自己投票
	rf.persist()

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
			LastLogIndex: rf.lastLogIndex(),
			LastLogTerm: rf.lastLogTerm(),
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
					rf.persist()
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
				rf.nextIndex[i] = rf.logLen()
			}
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
			}
			rf.cond.Broadcast() // 唤醒线程
			Debugf(dInfo, "S%d 成功当选, 唤醒 replicator", rf.me)
			rf.mu.Unlock()
		    return
		}

		rf.mu.Unlock()
	}

	Debugf(dVote, "S%d 选举失败, 获得 %d/%d 票", rf.me, grantedVotes, len(rf.peers))
}
