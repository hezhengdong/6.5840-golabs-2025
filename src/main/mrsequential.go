package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.5840/mr" // 相关数据结构和类型定义
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	// 检查命令行参数数量，至少需要插件文件名和一个输入文件
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	// 从插件文件中加载 Map 和 Reduce 函数
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{} // 存储所有 Map 阶段的输出

	// 循环读取所有输入文件
	for _, filename := range os.Args[2:] {
		// 打开文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		// 读取文件全部内容
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}

		// 关闭文件
		file.Close()

		// 1. Map 方法的输入是文件名与文件内容
		kva := mapf(filename, string(content))

		// 2. Map 方法的输出是 KeyValue 类型的切片
		// 将结果追加到 intermediate 中（如果是分布式，那就是放在当前目录的文件下）
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	// 3. 这是由 Map 输出向 Reduce 输入的转变
	// 对所有结果按 Key 排序
	sort.Sort(ByKey(intermediate))

	// 创建输出文件
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

    //
    // 对 intermediate[] 中每个不同的 Key 调用 Reduce，
    // 并将结果打印到 mr-out-0 文件中
    //
	i := 0
	for i < len(intermediate) { // 遍历中间结果
		// 找到具有相同 Key 的所有键值对的结束位置
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// 收集所有具有相同 Key 的 Value
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// 对 Key 和 Value 调用 Reduce 函数
		// 4. Reduce 方法的输入：参数1 key，参数2 该key对应所有value
		// 5. Reduce 方法的输出：只有一行字符串？目前来看是这样的
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		// 将格式化后的数据写入指定文件
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		// 移动到下一组 Key
		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	// 打开插件文件
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}

	// go中函数还能当返回值？如何调试go程序？

	// 查找插件中的 Map 符号（函数）
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	// 将查找到的符号转换为正确的函数类型
	mapf := xmapf.(func(string, string) []mr.KeyValue)

	// 查找插件中的 "Reduce" 符号（函数）
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	// 将查找到的符号转换为正确的函数类型
	reducef := xreducef.(func(string, []string) string)

	// 返回加载的 Map 和 Reduce 函数
	return mapf, reducef
}
