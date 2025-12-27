package raft

// 由于快照的存在，逻辑索引与物理索引无法对应，
// 需要经过lastIncludedIndex的映射，因此需要将raft实现分为日志存储层与算法逻辑层。
// 该文件是对 rf.logs 操作的封装。除本文件代码外，禁止其他文件代码直接操作 rf.logs

// 永远存在一个虚拟节点，位于索引 0，指向快照存储的最后一个条目。

func (rf *Raft) logLen() int {
	return rf.lastIncludedIndex + len(rf.logs)
}

func (rf *Raft) lastLogIndex() int {
    return rf.lastIncludedIndex + len(rf.logs) - 1
}

func (rf *Raft) lastLogTerm() int {
    return rf.logs[len(rf.logs) - 1].Term
}

func (rf *Raft) logTerm(logicIndex int) int {
	if logicIndex < rf.lastIncludedIndex {
		Debugf(dError, "S%d logicIndex %d < rf.lastIncludedIndex %d", rf.me, logicIndex, rf.lastIncludedIndex)
		panic("严重问题")
	}
	sliceIndex := logicIndex - rf.lastIncludedIndex
	return rf.logs[sliceIndex].Term
}

func (rf *Raft) logCommand(logicIndex int) interface{} {
	if logicIndex < rf.lastIncludedIndex {
		Debugf(dError, "S%d logicIndex %d < rf.lastIncludedIndex %d", rf.me, logicIndex, rf.lastIncludedIndex)
		panic("严重问题")
	}
	sliceIndex := logicIndex - rf.lastIncludedIndex
	return rf.logs[sliceIndex].Command
}

func (rf *Raft) logEntries(start, end int) []Log {
    sliceStart := start - rf.lastIncludedIndex
    sliceEnd := end - rf.lastIncludedIndex
    return rf.logs[sliceStart:sliceEnd]
}

func (rf *Raft) truncateLog(start, end int) {
	sliceStart := start - rf.lastIncludedIndex
    sliceEnd := end - rf.lastIncludedIndex
	rf.logs = rf.logs[sliceStart:sliceEnd]
}

func (rf *Raft) appendEntries(entries []Log) {
	rf.logs = append(rf.logs, entries...)
}

func (rf *Raft) appendNewEntry(log Log) {
    rf.logs = append(rf.logs, log)
}
