package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv" // FNV 哈希算法
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp" // 正则表达式
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// 使用 ihash(key) % NReduce 为 Map 发出的每个 KeyValue 选择 reduce 任务号。
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
// 函数参数与返回值是理解的重点
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 该方法由 main 直接调用
	// 所以该方法还承担着一个功能：初始化数据，控制整个 worker 的执行
	// 例如需要控制每隔一段时间向主节点请求任务

	// Your worker implementation here.
	for {
		worker(mapf, reducef)
		time.Sleep(1*time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 思路：
	// - 调用 RequestTaskRPC 获取执行任务必要的数据，根据类型进行必要处理
	// 必要数据：
	// - Map 任务：原始文本文件名（输入数据），nReduce（输出数据分区），mapId（输出数据命名）
	// - Reduce 任务：reduceId（输入数据）
	reply := RequestTaskRPC()
	switch reply.TaskType {
	case MapType:
		// 执行 map 任务
		mapCompleted := ExecuteMapTask(reply.MapInputFileName, reply.NReduce, reply.MapId, mapf)
		if mapCompleted {
			// 传递任务类型与id
			ReportTaskDoneRPC("map", reply.MapId)
		}
	case ReduceType:
		// 执行 reduce 任务
		reduceCompleted := ExecuteReduceTask(reply.ReduceId, reducef)
		if reduceCompleted {
			// 传递任务类型与id
			ReportTaskDoneRPC("reduce", reply.ReduceId)
		}
	case NoTask:
		// 暂时没有任务，等待一段时间
		time.Sleep(3 * time.Second)
	case Exit:
		// 终止当前进程（操作系统启动一个可执行文件就是开启了一个进程）
		os.Exit(0)
	}
}

func ExecuteMapTask(filename string, nReduce int, mapId int, mapf func(string, string) []KeyValue) bool {
	// 抄 mrsequential.go：执行 mapf
	file, err := os.Open(filename) // 默认为当前目录即可，基于程序运行时所处的路径判断
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	// 用于缓存 map 任务的输出数据，key 为 filename，value 为 KeyValue 切片
	cacheMap := make(map[string][]KeyValue)

	// 根据 key 的哈希值，对 map 输出数据进行分区
	for _, keyValue := range kva {
		reduceId := ihash(keyValue.Key) % nReduce // 计算该键值对应当存储进哪个分区
		filename := fmt.Sprintf("mr-%v-%v", mapId, reduceId) // 该键值对所属的文件名
		// 先检查是否存在所属文件
		// 如果不存在，初始化键值对
		// 如果存在，在对应切片中追加数据
		_, exist := cacheMap[filename] // go map 没有 contains 内置函数
		if !exist {
			cacheMap[filename] = []KeyValue{keyValue}
		} else {
			cacheMap[filename] = append(cacheMap[filename], keyValue)
		}
	}

	// 成功分区且缓存后，将数据持久化至本地磁盘
	for filename, slice := range cacheMap {
		// 创建文件
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("创建文件 %v 失败\n", filename)
		}
		defer file.Close()
		// 遵循 Lab1 Hints 思路：将数据以 json 格式写入文件
		enc := json.NewEncoder(file)
		for _, kv := range slice {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("写入文件 %v 失败\n", filename)
			}
		}
	}

	return true
}

// 抄 mrsequential.go：创建支持排序的数据结构
// 相当于实现了 sort.Interface 接口，能够使用 sort.Sort 函数排序
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ExecuteReduceTask(reduceId int, reducef func(string, []string) string) bool {
	// 存储 reduce 输入数据
	intermediate := []KeyValue{}

	// 读取文件内容，获取 reduce 输入数据
	dir := "." // 当前目录
	pattern := fmt.Sprintf(`^mr-\d+-%v$`, reduceId) // 表示匹配 mr-数字-[reduceId]
    files, err := findMatchingFiles(dir, pattern) // 获取文件数，用于获取后续文件名
	if err != nil {
		log.Fatalf("程序终止，错误: %v", err)
	}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()
		// 遵循 Lab1 Hints 思路：读取 json 格式的文件数据
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// 抄 mrsequential.go：执行 reducef
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", reduceId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// 如果执行到了这里，说明 reducef 执行成功，将相关的中间文件删除掉
	for _, filename := range files {
		err := os.Remove(filename)
		if err != nil {
			log.Fatalf("reduce 任务 %v 中，文件 %v 删除失败: %v\n", reduceId, filename, err)
		}
	}
	log.Printf("reduce 任务 %v 执行成功，文件 %v 删除成功\n", reduceId, files)

	return true
}

// 寻找指定目录下符合正则规则的文件名（AI生成的）
func findMatchingFiles(dir string, pattern string) ([]string, error) {
    // 编译正则表达式
    re, err := regexp.Compile(pattern)
    if err != nil {
        return nil, fmt.Errorf("无效的正则表达式模式: %v", err)
    }
	
	// 读取目录
    entries, err := os.ReadDir(dir)
    if err != nil {
        return nil, fmt.Errorf("无法读取目录 %s: %v", dir, err)
    }

	var files []string // 用于存储匹配的文件名

	// 遍历匹配结果，过滤出文件（排除目录）
	for _, path := range entries {
		// 只匹配文件，跳过目录
		if path.IsDir() {
            continue
        }

		// 检查文件名是否匹配正则表达式
		if re.MatchString(path.Name()) {
            files = append(files, path.Name())
        }
	}

	log.Printf("当前目录下符合 '%v' 格式的文件为: %v", pattern, files)

	return files, nil
}

// 该方法应当是心跳的同时附带请求任务的职责，而不是请求任务附带心跳

// 请求任务的方法，同时也肩负着心跳的职责
// 参数：worker pid（如果本地有已完成的任务，参数携带上一次分配任务的id与true）
// 返回值：返回任务id，输入文件
// 发生错误代表着任务与心跳都无效
func RequestTaskRPC() RequestTaskReply {
	args := struct{}{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		switch reply.TaskType {
		case MapType:
			log.Printf("RequestTask RPC 方法调用成功, %v 任务 %v 被 worker 请求成功\n", reply.TaskType, reply.MapId)
		case ReduceType:
			log.Printf("RequestTask RPC 方法调用成功, %v 任务 %v 被 worker 请求成功\n", reply.TaskType, reply.ReduceId)
		case NoTask:
			log.Printf("RequestTask RPC 方法调用成功, 没有任务可被请求, 阻塞等待\n")
		case Exit:
			log.Printf("RequestTask RPC 方法调用成功, 任务类型为 %v, worker 退出", reply.TaskType)
		default:
			log.Fatalf("RequestTask RPC 方法调用成功, 未定义的类型")
		}
		return reply
	} else {
		log.Fatalf("RequestTask RPC 方法调用失败, 默认MapReduce作业完成, Coordinator 已终止, 该 Worker 也终止\n")
		return RequestTaskReply{}
	}
}

// 还是接耦开来，不能一个方法什么都干了，自己也不能逃避写rpc方法
// 或者说这个方法有存在的必要吗？分配给worker后能够直接默认其执行成功吗？
// 参数为任务id及其完成情况
// 没有返回值
func ReportTaskDoneRPC(taskType string, taskId int) {
	args := ReportTaskDoneArgs {
		TaskType: taskType,
		TaskId: taskId,
	}
	reply := struct{}{}
	ok := call("Coordinator.ReportTaskDone", &args, &reply)
	if ok {
		log.Printf("ReportTaskDone RPC 方法调用成功, %v 任务 %v 成功执行完成并告知 Coordinator\n", taskType, taskId)
	} else {
		log.Printf("ReportTaskDone RPC 方法调用失败, 涉及 %v 任务 %v\n", taskType, taskId)
	}
}

// 示例函数，展示如何向协调者进行 RPC 调用
// RPC 参数和回复类型在 rpc.go 中定义
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// 声明一个参数结构体
	args := ExampleArgs{}

	// 填充参数
	args.X = 99

	// 声明一个回复结构体
	reply := ExampleReply{} // rpc 调用的返回值在这里
	// 获取应该当作参数传递？在worker与coordinator都设置
	// requestTask方法，保持参数与返回值一致，从而模仿出
	// “像调用本地方法一样调用远程方法”的效果。

    // 关键，调用主节点的远程方法

	// 发送RPC请求，等待回复
	// "Coordinator.Example"告诉接收服务器我们
	// 想调用RPC服务器上Coordinator结构体的Example()方法
	ok := call("Coordinator.Example", &args, &reply) // RPC 方法名称、传递的参数、获取的回复
	if ok {
		// reply.Y should be 100.
		// rpc返回值处理
		fmt.Printf("reply.Y %v\n", reply.Y)
		fmt.Printf("reply.taskType %v reply.taskDataSource %v\n", reply.taskType, reply.taskDataSource)
		if reply.taskType == "map" {
			//mapf();
		} else if reply.taskType == "reduce" {
			//reducef();
		} else {
			log.Println("没有该类型")
		}
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// 底层 RPC 调用函数
// 向协调者发送 RPC 请求，等待响应
// 通常返回 true
// 如果出现问题，则返回 false
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// 获取协调者套接字名称
	sockname := coordinatorSock()
	// 通过 Unix 域套接字连接 RPC 服务器
	// var c Client。显示声明类型也有优点，更根据类型名推断变量作用，例如这里的服务器
	c, err := rpc.DialHTTP("unix", sockname) // 尝试建立连接
	if err != nil { // 错误处理
		log.Fatal("dialing:", err)
	}
	defer c.Close() // defer 相当于 Java finally

	// 执行 RPC 调用
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
