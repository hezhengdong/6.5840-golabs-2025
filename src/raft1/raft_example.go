package raft

// ====================用到的终端命令====================

// 创建虚拟环境
// python3 -m venv venv
// 激活虚拟环境
// source venv/bin/activate
// 安装 typer 库
// pip install typer

// 在终端执行一次
// time VERBOSE=1 go test -run 3A | python dslog.py -c 3
// 执行多次，如此处 100 个线程执行 200 次任务，错误日志输出到指定目录中
// VERBOSE=1 python dstest.py 3A -n 200 -p 100 -o res3A
// 以分列形式在终端查看已有文件
// cat res3A/3A_0.log | python dslog.py -c 3
// 此外，需要根据节点数修改显示的列数

// ====================框架代码示例====================

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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persistBak() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer) // 创建空的字节缓冲区，用于写入数据，存储编码后的数据
	// e := labgob.NewEncoder(w) // 基于缓冲区 w 创建一个 labgob 编码器
	// e.Encode(rf.xxx) // 编码数据结构
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes() // 获取缓冲区内的所有字节，即为编码后的 raft 持久话状态
	// rf.persister.Save(raftstate, nil) // 保存字节
}

// restore previously persisted state.
func (rf *Raft) readPersistBak(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data) // 用已有数据创建字节缓冲区
	// d := labgob.NewDecoder(r) // 基于缓冲区的 labgob 编码器
	// var xxx // 接收解码数据的变量
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil { // 解码数据
	//   error...
	// } else {
	//   rf.xxx = xxx // 解码成功，赋值给 Raft 字段
	//   rf.yyy = yyy
	// }
}
