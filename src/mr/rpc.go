package mr

//
// RPC definitions.
//
// remember to capitalize all names.
// 所有名称首字母都要大写？因为 RPC 规定
//

import "os" // 操作系统功能（意思是这个包提供的都是操作系统暴露的对应接口调用吗？）
import "strconv" // 字符串转换

//
// example to show how to declare the arguments
// and reply for an RPC.
// 示例展示如何声明 RPC 的参数和回复
//

// 示例参数
type ExampleArgs struct {
	X int
}

// 示例回复
type ExampleReply struct {
	Y int
	taskDataSource string
	taskType string
}

// Add your RPC definitions here.

type TaskType int
const (
	MapType TaskType = iota
	ReduceType
	NoTask
	Exit
)
func (t TaskType) String() string {
    switch t {
    case MapType:
        return "Map"
    case ReduceType:
        return "Reduce"
    case NoTask:
        return "NoTask"
    case Exit:
        return "Exit"
    default:
        panic("未定义的类型")
    }
}

type RequestTaskReply struct {
	TaskType TaskType // 任务类型(map reduce)
	MapInputFileName string // map任务输入文件名(map)
	NReduce int // reduce任务总数，map需要根据该参数将输出划分(map)
	MapId int // map需要根据mapid与reduceid生成中间文件（这里的reduceid会根据key的哈希值自动生成）(map)
	ReduceId int // reduce需要根据reduceid获取中间文件作为输入(reduce)
}

type ReportTaskDoneArgs struct {
	TaskType string
	TaskId int
}

// 生成一个唯一的 Unix 域套接字名称
// 在 /var/tmp 目录中，用于协调者
// 不能使用当前目录，因为 Athena AFS 不支持 Unix 域套接字
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	// 套接字名称前缀
	s := "/var/tmp/5840-mr-"
	// 添加当前用户 ID 确保唯一性
	s += strconv.Itoa(os.Getuid())
	// 返回完整的套接字名称
	return s
}
