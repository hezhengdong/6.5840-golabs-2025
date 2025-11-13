package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"fmt"
)

// 判断任务是否超时的阈值
const TaskTimeout = 10 * time.Second
// 全局 ReduceId，分配给请求的 worker
const ReduceId = 0

// 执行阶段
type Phase int
const (
	MapPhase Phase = iota
	ReducePhase
	CompletedPhase
)

type MapTask struct {
	TaskId int
	FileName string // 输入文件名
	StartTime time.Time // 开始执行时间，用于超时检测
}

type ReduceTask struct {
	TaskId int
	// 不需要这个属性，Reduce的输入数据来自很多文件，能够根据id识别
	// 所以该属性需要删除掉，但是测试方法依赖这个属性，所以先保留下来吧，看看怎么改
	FileName string
	StartTime time.Time
}

// 协调者结构体定义，负责分配任务和管理 MapReduce 作业
type Coordinator struct {
	// 任务状态管理
	IdleMapTasks chan MapTask
	InProcessMapTasks map[int]MapTask
	CompletedMapTasks map[int]struct{}
	IdleReduceTasks chan ReduceTask
	InProcessReduceTasks map[int]ReduceTask
	CompletedReduceTasks map[int]struct{}

	// 阶段控制
	Phase Phase

	// 任务数量, 与已完成任务数作对比, 判断当前阶段是否完成
	NMap int
	NReduce int

	// 并发控制
	mu sync.Mutex // 互斥锁
}

func (c *Coordinator) PrintStatusSimple() {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    fmt.Printf("Coordinator状态:\n")
    fmt.Printf("阶段: %v, NMap: %d, NReduce: %d\n", c.Phase, c.NMap, c.NReduce)
    fmt.Printf("Map任务 - 空闲: %d, 处理中: %d, 完成: %d\n", 
        len(c.IdleMapTasks), len(c.InProcessMapTasks), len(c.CompletedMapTasks))
    fmt.Printf("Reduce任务 - 空闲: %d, 处理中: %d, 完成: %d\n", 
        len(c.IdleReduceTasks), len(c.InProcessReduceTasks), len(c.CompletedReduceTasks))
}

// 超时检测并重新分配超时任务（需要为其编写一个测试方法）
func (c *Coordinator) HandleTimedOutTasks() {
	now := time.Now()

	switch c.Phase {
	case MapPhase:
		// 需要做什么？
		// 遍历所有执行中的map任务
		for mapTaskId, mapTask := range c.InProcessMapTasks {
			// 逐个检查任务的当前距开始执行的时间，并判断其是否超时
			// 如果超时，将其从“执行中”移除，放入“空闲”管道中
			if now.Sub(mapTask.StartTime) > TaskTimeout {
				log.Printf("超时检测: Map 任务 %v 执行时间超过阈值, 将其移入空闲队列\n", mapTaskId)
				mapTask.StartTime = time.Time{} // 将任务开始执行时间重置为空
				c.IdleMapTasks <- mapTask
				delete(c.InProcessMapTasks, mapTaskId)
			}
			// 如果未超时，什么都不做
		}
	case ReducePhase:
		for reduceTaskId, reduceTask := range c.InProcessReduceTasks {
			if now.Sub(reduceTask.StartTime) > TaskTimeout {
				log.Printf("超时检测: Reduce 任务 %v 执行时间超过阈值, 将其移入空闲队列\n", reduceTaskId)
				reduceTask.StartTime = time.Time{}
				c.IdleReduceTasks <- reduceTask
				delete(c.InProcessReduceTasks, reduceTaskId)
			}
		}
	case CompletedPhase:
		// 报错，完成阶段不需要调用本方法
		log.Fatalf("参数错误：没必要的调用 %v\n", c.Phase)
	default:
		// 报错，未定义的阶段
		log.Fatalf("参数错误：未定义的阶段 %v\n", c.Phase)
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// 两个参数分别是rpc的参数与返回值
// 注意，返回值在rpg.go中指定，在coordinator与worker的rpc方法参数重使用
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	reply.taskType = "map"
	reply.taskDataSource = "pg-grimm.txt"
	return nil
}

// 这个方法需要参数吗？
// 不需要参数，需要返回值
// 逻辑：根据当前所处阶段，判断应当为其分配什么任务
// 如果是map阶段，为其分配map任务
// - 如果没有任务，那么为其传送noTask类型
// 如果是reduce阶段，为其分配reduce任务
// - 如果没有任务，那么为其传送noTask类型
// 如果是完成阶段，为其分配exit任务（有必要吗？没必要吧。）
func (c *Coordinator) RequestTask(args *struct{}, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.Phase {
	case MapPhase:
		// 检查管道中是否存在元素，如果不存在，返回noTask类型
		if len(c.IdleMapTasks) == 0 {
			reply.TaskType = NoTask
			return nil
		}
		// 从管道中获取任务
		mapTask := <-c.IdleMapTasks
		// 更新执行时间
		mapTask.StartTime = time.Now()
		// 将其存入 InProcessMapTasks
		c.InProcessMapTasks[mapTask.TaskId] = mapTask
		// 初始化返回值
		reply.TaskType = MapType
		reply.MapInputFileName = mapTask.FileName
		reply.NReduce = c.NReduce
		reply.MapId = mapTask.TaskId
		return nil
	case ReducePhase:
		if len(c.IdleReduceTasks) == 0 {
			reply.TaskType = NoTask
			return nil
		}
		reduceTask := <-c.IdleReduceTasks
		reduceTask.StartTime = time.Now()
		c.InProcessReduceTasks[reduceTask.TaskId] = reduceTask
		reply.TaskType = ReduceType
		reply.ReduceId = reduceTask.TaskId
		return nil
	case CompletedPhase:
		reply.TaskType = Exit
		return nil
	default:
		panic(fmt.Sprintf("参数错误：未定义的阶段: %v", c.Phase))
	}
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskType := args.TaskType
	taskId := args.TaskId

	switch taskType {
	case "map":
		// 检查该任务是否在执行中 map 存在。如果不存在，无视本次请求
		if _, exist := c.InProcessMapTasks[taskId]; !exist {
			log.Printf("任务 %v %v 已被处理。为防止重复消费，无视本次请求\n", taskType, taskId)
			return nil
		}
		// 将任务从执行中 map 移除
		delete(c.InProcessMapTasks, taskId)
		// 添加至已完成任务
		c.CompletedMapTasks[taskId] = struct{}{}
		// 如果完成的任务数 == 总任务数，则 map 阶段结束
		if len(c.CompletedMapTasks) == c.NMap && len(c.IdleMapTasks) == 0 && len(c.InProcessMapTasks) == 0 {
			c.Phase = ReducePhase
			// 初始化填充 Reduce 任务
			for i := range c.NReduce {
				c.IdleReduceTasks <- ReduceTask{
					TaskId: i,
				}
			}
		}
	case "reduce":
		if _, exist := c.InProcessReduceTasks[taskId]; !exist {
			log.Printf("任务 %v %v 已被处理。为防止重复消费，无视本次请求\n", taskType, taskId)
			return nil
		}
		delete(c.InProcessReduceTasks, taskId)
		c.CompletedReduceTasks[taskId] = struct{}{}
		if len(c.CompletedReduceTasks) == c.NReduce && len(c.IdleReduceTasks) == 0 && len(c.InProcessReduceTasks) == 0 {
			c.Phase = CompletedPhase
		}
	default:
		panic("参数错误：未定义的类型")
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c) // 注册 RPC 服务
	rpc.HandleHTTP() // 使用 HTTP 协议处理 RPC
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock() // 调用 rpc.go 中的函数，生成唯一的 Unix 域套接字名称
	os.Remove(sockname) // 移除已存在的套接字文件（意思是这种进程间通信的方式是通过创建一个文件来传输数据的）
	l, e := net.Listen("unix", sockname) // 监听 Unix 域套接字
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // 启动 HTTP 服务
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// main/mrcoordinator.go 定期调用 Done() 来确认整个任务是否已完成
//
func (c *Coordinator) Done() bool {
	return c.Phase == CompletedPhase
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// 两个参数：所有输入文件名，reduce 任务数量
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 初始化 Coordinator 实例
	c := Coordinator{
		IdleMapTasks:         make(chan MapTask, len(files)),
        InProcessMapTasks:    make(map[int]MapTask),
        CompletedMapTasks:    make(map[int]struct{}),
        IdleReduceTasks:      make(chan ReduceTask, nReduce),
        InProcessReduceTasks: make(map[int]ReduceTask),
        CompletedReduceTasks: make(map[int]struct{}),
        Phase:                MapPhase,
		NMap:                 len(files),
        NReduce:              nReduce,
	}

	// 初始化填充 Map 任务
	for i, file := range files {
		c.IdleMapTasks <- MapTask{
			TaskId: i,
			FileName: file,
		}
	}

	// 启动一个线程检测超时任务
	go func(){
		for {
			if c.Phase == CompletedPhase {
				log.Printf("MapReduce作业已完成, 超时检测任务终止")
				break
			}
			c.HandleTimedOutTasks()
			time.Sleep(1*time.Second)
		}
	}()

	// 启动 RPC 服务器
	c.server()
	return &c
}
