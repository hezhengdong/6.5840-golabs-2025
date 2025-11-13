package main

//
// a MapReduce pseudo-application that sometimes crashes,
// and sometimes takes a long time,
// to test MapReduce's ability to recover.
//
// go build -buildmode=plugin crash.go
//

import "6.5840/mr"
import crand "crypto/rand" // 加密安全的随机数生成包
import "math/big" // 大数运算包
import "strings"
import "os"
import "sort"
import "strconv" // 字符串转换包
import "time"

func maybeCrash() {
	max := big.NewInt(1000)
	// 生成一个 0-999 的加密安全随机数
	rr, _ := crand.Int(crand.Reader, max)
	if rr.Int64() < 330 { // 33% 概率崩溃
		// crash!
		os.Exit(1)
	} else if rr.Int64() < 660 { // 33% 概率延迟
		// delay for a while.
		maxms := big.NewInt(10 * 1000)
		// 0-10000随机数
		ms, _ := crand.Int(crand.Reader, maxms)
		time.Sleep(time.Duration(ms.Int64()) * time.Millisecond)
	}
}

func Map(filename string, contents string) []mr.KeyValue {
	maybeCrash()

	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{"a", filename})
	kva = append(kva, mr.KeyValue{"b", strconv.Itoa(len(filename))})
	kva = append(kva, mr.KeyValue{"c", strconv.Itoa(len(contents))})
	kva = append(kva, mr.KeyValue{"d", "xyzzy"})
	return kva
}

func Reduce(key string, values []string) string {
	maybeCrash()

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
