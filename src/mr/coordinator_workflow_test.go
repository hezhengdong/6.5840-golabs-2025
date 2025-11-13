package mr

import (
	"fmt"
	"time"
	"math/rand"
	"testing"
	"os"
)

func TestCoordinatorWorkflow(t *testing.T) {
	// 初始化
	files := []string{
    	"pg-being_ernest.txt",
    	"pg-dorian_gray.txt", 
    	"pg-frankenstein.txt",
    	"pg-grimm.txt",
    	"pg-huckleberry_finn.txt",
    	"pg-metamorphosis.txt",
    	"pg-sherlock_holmes.txt", 
    	"pg-tom_sawyer.txt",
	}
	NReduce := 10
	c := MakeCoordinator(files, NReduce)

	// 随机数种子
	rand.Seed(time.Now().UnixNano())

	// Map 阶段
	for {
		// 如果出发该条件，说明 Map 任务全部执行完成
		if c.Phase == ReducePhase {
			fmt.Printf("Map 阶段完成\n")
			break
		}
		// 模拟分布式处理，worker 异步执行任务
		go c.MockWorker()
		// 模拟随机 worker 的随机请求
		randomNum := rand.Intn(200) + 1
		time.Sleep(time.Duration(randomNum) * time.Millisecond)
		// 打印当前 Coordinator 的状态
		c.PrintStatusSimple()
	}

	fmt.Printf("\nMap 阶段最终的状态：\n")
	c.PrintStatusSimple()

	// Reduce 阶段
	for {
		if c.Phase == CompletedPhase {
			fmt.Printf("Reduce 阶段完成\n")
			break
		}
		go c.MockWorker()
		randomNum := rand.Intn(200) + 1
		time.Sleep(time.Duration(randomNum) * time.Millisecond)
		c.PrintStatusSimple()
	}

	fmt.Printf("\nReduce 阶段最终的状态：\n")
	c.PrintStatusSimple()

	if !c.Done() {
		t.Error("MapReduce 作业未完成")
	}
}

func (c *Coordinator) MockWorker() {
	reply := RequestTaskReply{}
	c.RequestTask(&struct{}{}, &reply)
	switch reply.TaskType {
	case MapType:
		// 模拟任务处理
		fmt.Printf("Map任务%d开始处理: 文件%s\n", reply.MapId, reply.MapInputFileName)
		randomNum := rand.Intn(200) + 1 // 随机时间 1-200ms
		time.Sleep(time.Duration(randomNum) * time.Millisecond)
		// 报告任务完成
		args := ReportTaskDoneArgs{
			TaskType: "map",
			TaskId: reply.MapId,
		}
		c.ReportTaskDone(&args, &struct{}{})
	case ReduceType:
        fmt.Printf("Reduce任务%d开始处理: 文件%s\n", reply.ReduceId, fmt.Sprintf("mr-*-%v", reply.ReduceId))
		randomNum := rand.Intn(200) + 1
		time.Sleep(time.Duration(randomNum) * time.Millisecond)
		args := ReportTaskDoneArgs{
			TaskType: "reduce",
			TaskId: reply.ReduceId,
		}
		c.ReportTaskDone(&args, &struct{}{})
	case NoTask:
		// 暂时没有任务，等待一段时间
		fmt.Printf("%v 阶段暂时没有任务，阻塞等待", c.Phase)
		time.Sleep(1 * time.Second)
	case Exit:
		// 终止当前进程
		os.Exit(0)
	}
}