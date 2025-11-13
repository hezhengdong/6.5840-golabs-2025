package main

//
// a MapReduce pseudo-application that counts the number of times map/reduce
// tasks are run, to test whether jobs are assigned multiple times even when
// there is no failure.
//
// go build -buildmode=plugin crash.go
//

import "6.5840/mr"
import "math/rand"
import "strings"
import "strconv"
import "time"
import "fmt"
import "os"
import "io/ioutil"

var count int

func Map(filename string, contents string) []mr.KeyValue {
	// 在当前目录下创建文件，工作者进程 ID 为唯一标识
	me := os.Getpid()
	f := fmt.Sprintf("mr-worker-jobcount-%d-%d", me, count)
	count++
	err := ioutil.WriteFile(f, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}
	// 随机延迟
	time.Sleep(time.Duration(2000+rand.Intn(3000)) * time.Millisecond)
	// 返回相同的键值对
	// 意味着所有 Map 任务的输出最终会被同一个 Reduce 任务处理
	return []mr.KeyValue{mr.KeyValue{"a", "x"}}
}

func Reduce(key string, values []string) string {
	// 读取当前目录下所有文件
	files, err := ioutil.ReadDir(".")
	if err != nil {
		panic(err)
	}
	// 统计文件数量
	invocations := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "mr-worker-jobcount") {
			invocations++
		}
	}
	// 返回统计结果
	return strconv.Itoa(invocations)
}
