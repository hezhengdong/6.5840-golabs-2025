package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.5840/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	// 创建协调器实例
	m := mr.MakeCoordinator(os.Args[1:], 10)

	// 主循环：持续检查 MapReduce 作业是否完成
	for m.Done() == false {
		// 每秒检查一次作业状态
		time.Sleep(time.Second)
	}

	// 最后 worker 需要根据 coordinator 的信息判断 mp 是否完成
	time.Sleep(time.Second)
}
