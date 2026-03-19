package rsm

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"

)

var useRaftStateMachine bool // to plug in another raft besided raft1
// 也就是说该 kvserver 底层还能切换不同的 raft 实现，只要保证接口相同即可。


type Op struct { // Operation
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Id  int // 唯一标识符
	Req any // 操作命令本身
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

// Raft 状态机
type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	channels     map[int]chan Result
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		channels:     make(map[int]chan Result),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.Reader()
	return rsm
}

type Result struct {
	Id int
	Value any
}

// 将状态机相关的所有操作交由 reader goroutine
func (rsm *RSM) Reader() {
    var index int
	// 监听 applyCh，获取数据
    for msg := range rsm.applyCh {
		// 如果该消息是已提交的日志条目
        if msg.CommandValid {
            index = msg.CommandIndex
            op := msg.Command.(Op)

            // 1. 状态机必须被串行单线程执行，为了保证集群内所有状态机的绝对一致
            value := rsm.sm.DoOp(op.Req)

            // 2. 将执行结果传递给对应的 Submit 调用
            rsm.mu.Lock()
            ch, exists := rsm.channels[index]
			if exists {
                delete(rsm.channels, index)
            } // 如果当前节点是 follower，意味着不存在对应 channel，无需处理
            rsm.mu.Unlock()

            // 3. 传递数据，之所以在锁外是为了防阻塞
            if exists {
                ch <- Result{Id: op.Id, Value: value}
            }
        }
        // 如果发来的消息是快照
        if msg.SnapshotValid {
            // 这是第二种情况，先把第一种做完再说
            rsm.sm.Restore(msg.Snapshot)
            index = msg.SnapshotIndex // 任期没什么用
        }
        // 如果 Raft 占用内存超过阈值，则进行快照
        if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
            snapshot := rsm.sm.Snapshot()
            rsm.rf.Snapshot(index, snapshot)
        }
    }

    // 调用 rf.Kill()，applyCh 被关闭，进而结束上面的循环
    rsm.mu.Lock()
    for idx, ch := range rsm.channels {
        close(ch)
        delete(rsm.channels, idx)
    }
    rsm.mu.Unlock()
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here

    id := rand.Int()
    op := Op{Id: id, Req: req}
    rsm.mu.Lock()
    index, _, isLeader := rsm.rf.Start(op) // 由于 raft.applier 返回命令时并不会返回 term，因此 term 没什么用，被忽略
    if !isLeader {
        rsm.mu.Unlock()
        return rpc.ErrWrongLeader, nil
    }

    // 注意：channel 的缓冲区至少为 1
    // 原因：select 本身不持有锁，如果 server 正好不是 leader，且 reader 即将 channel <- Result 时，select 恰好进入 time 分支删除 channel，reader 尝试写入不存在的 channel 就会 panic
	// 结果：保留大小为 1 的缓冲区，防止 reader panic
    ch := make(chan Result, 1) 
    rsm.channels[index] = ch
    rsm.mu.Unlock()

	for {
        select {
        case result, ok := <-ch:
            if !ok { // channel 已关闭
                return rpc.ErrWrongLeader, nil
            }
            if result.Id != id { // 操作未被写入 raft 层
                return rpc.ErrWrongLeader, nil
            }
            return rpc.OK, result.Value

        // 循环检查自己是否是 Leader
        case <-time.After(10 * time.Millisecond):
            // 如果不是的话，删除并关闭 channel
            _, stillLeader := rsm.rf.GetState()
            if !stillLeader {
                rsm.mu.Lock()
                delete(rsm.channels, index)
                close(ch) 
                rsm.mu.Unlock()
                return rpc.ErrWrongLeader, nil
            }
        }
    }
}