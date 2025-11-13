package mr

import (
	"log"
	"testing"
	"time"
)

var mapType string = "map"
var reduceType string = "reduce"

// ==================== HandleTimeOutTask 函数测试 ====================

// 思路：初始，分别为 Map 与 Reduce 分配一个空闲任务，
// 三个执行中方法（有两个超时）。随后执行超时检测，检查
// 执行结果是否符合预期
func TestHandleTimeOutTask(t *testing.T) {
	// 创建 Coordinator 实例
	c := &Coordinator{
		IdleMapTasks: make(chan MapTask, 10),
		InProcessMapTasks: make(map[int]MapTask),
		CompletedMapTasks: make(map[int]struct{}),
		IdleReduceTasks: make(chan ReduceTask, 10),
		InProcessReduceTasks: make(map[int]ReduceTask),
		CompletedReduceTasks: make(map[int]struct{}),
		Phase: MapPhase,
	}

	// 添加任务
	c.addIdleTasks()
	c.addInProcessTasks()

	log.Printf("# 超时检测 Map 任务前\n")
	log.Printf("空闲 Map 任务数：%v\n", len(c.IdleMapTasks))
	log.Printf("执行中的 Map 任务：%v\n", c.InProcessMapTasks)

	log.Printf("# 超时检测 Map 任务后\n")
	c.HandleTimedOutTasks()
	log.Printf("空闲 Map 任务数：%v\n", len(c.IdleMapTasks))
	log.Printf("执行中的 Map 任务：%v\n", c.InProcessMapTasks)

	log.Printf("# 超时检测 Reduce 任务前\n")
	log.Printf("空闲 Reduce 任务数：%v\n", len(c.IdleReduceTasks))
	log.Printf("执行中的 Reduce 任务：%v\n", c.InProcessReduceTasks)

	log.Printf("# 超时检测 Reduce 任务后\n")
	c.Phase = ReducePhase
	c.HandleTimedOutTasks()
	log.Printf("空闲 Reduce 任务数：%v\n", len(c.IdleReduceTasks))
	log.Printf("执行中的 Reduce 任务：%v\n", c.InProcessReduceTasks)

	if len(c.IdleMapTasks) != 3 || len(c.IdleReduceTasks) != 3 {
		t.Error("实际空闲任务数与预期不符")
	}
}

func (c *Coordinator) addIdleTasks() {
	// 添加一个空闲的 Map 任务
	mapTask := MapTask{
		TaskId:     1,
		FileName:   "data1.txt",
	}
	c.IdleMapTasks <- mapTask

	// 添加一个空闲的 Reduce 任务
	reduceTask := ReduceTask{
		TaskId:     1,
		FileName:   "output1.txt",
	}
	c.IdleReduceTasks <- reduceTask
}

// 添加执行中任务
func (c *Coordinator) addInProcessTasks() {
	now := time.Now()
	
	// 添加三个 Map 任务（两个超时，一个正常）
	c.addInProcessTask(mapType, 2, "data2.txt", now.Add(-15*time.Second)) // 超时（15秒前）
	c.addInProcessTask(mapType, 3, "data3.txt", now.Add(-5*time.Second))  // 正常（5秒前）
	c.addInProcessTask(mapType, 4, "data4.txt", now.Add(-12*time.Second)) // 超时（12秒前）

	// 添加三个 Reduce 任务（两个超时，一个正常）
	c.addInProcessTask(reduceType, 2, "output2.txt", now.Add(-20*time.Second)) // 超时
	c.addInProcessTask(reduceType, 3, "output3.txt", now.Add(-3*time.Second))  // 正常
	c.addInProcessTask(reduceType, 4, "output4.txt", now.Add(-11*time.Second)) // 超时
}

func (c *Coordinator) addInProcessTask(taskType string, id int, filename string, startTime time.Time) {
	switch taskType {
	case mapType:
		task := MapTask{
			TaskId: id,
			FileName: filename,
			StartTime: startTime,
		}
		c.InProcessMapTasks[id] = task
	case reduceType:
		task := ReduceTask{
			TaskId: id,
			FileName: filename,
			StartTime: startTime,
		}
		c.InProcessReduceTasks[id] = task
	default:
		log.Fatalln("参数错误，类型不存在")
	}
}
