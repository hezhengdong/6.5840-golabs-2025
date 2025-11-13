package main

import "6.5840/mr"
import "plugin"
import "os"
import "fmt"
import "log"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	// 从插件文件中加载 Map 和 Reduce 函数
	mapf, reducef := loadPlugin(os.Args[1])

	// 手动执行所有 map 与 reduce 任务，测试 worker 内逻辑是否正确
	filenames := []string {
		"pg-being_ernest.txt",
		"pg-dorian_gray.txt",
		"pg-frankenstein.txt",
		"pg-grimm.txt",
		"pg-huckleberry_finn.txt",
		"pg-metamorphosis.txt",
		"pg-sherlock_holmes.txt",
		"pg-tom_sawyer.txt",
	}
	for i, filename := range filenames {
    	mr.ExecuteMapTask(filename, 10, i, mapf)
	}
	for i := 0; i < 10; i++ {
		mr.ExecuteReduceTask(i, reducef)
	}
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
