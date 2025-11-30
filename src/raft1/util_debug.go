package raft

import (
    "fmt"
    "log"
    "os"
    "strconv"
    "time"
)

// 参考自 http://blog.josejg.com/debugging-pretty/ The Go side 章节

type logTopic string
const (
    dClient  logTopic = "CLNT" // 客户端操作
    dCommit  logTopic = "CMIT"
    dError   logTopic = "ERRO"
    dInfo    logTopic = "INFO" // 一般信息
    dLeader  logTopic = "LEAD" // 领导者状态变更
    dLog     logTopic = "LOG1" // 日志发送操作
    dLog2    logTopic = "LOG2" // 日志保存/应用操作
    dPersist logTopic = "PERS" // 持久化操作
    dTimer   logTopic = "TIMR" // 定时器操作
    dVote    logTopic = "VOTE" // 选举投票操作
    dWarn    logTopic = "WARN"
)

// 获取日志级别
func getVerbosity() int {
    v := os.Getenv("VERBOSE") // 读取环境变量
    level := 0 // 默认级别为 0，不输出调试日志
    if v != "" { // 如果环境变量不为空，尝试解析
        var err error
        level, err = strconv.Atoi(v) // 将字符串转换为整数
        if err != nil { // 转换失败则报错退出
            log.Fatalf("Invalid verbosity %v", v)
        }
    }
    return level
}

var debugStart time.Time
var debugVerbosity int

// 该函数在包被导入时，会自动在 main 函数之前被执行
func init() {
	debugVerbosity = getVerbosity() // 初始化日志级别
	debugStart = time.Now() // 记录程序启动时间

	// 配置标准日志库的输出格式：
    // 移除默认的日期和时间前缀，因为我们使用自定义的时间戳格式
    // log.Flags()获取当前标志，然后通过按位清除日期和时间标志
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// Debug函数：自定义调试日志输出函数
// topic: 日志主题，用于分类和过滤
// format: 格式化字符串，与fmt.Printf类似
// a: 可变参数，用于格式化字符串中的占位符
func Debugf(topic logTopic, format string, a ...interface{}) {
    // 检查是否启用调试输出
	if debugVerbosity >= 1 {
		// 计算从启动到现在的微妙数
        time := time.Since(debugStart).Microseconds()
        time /= 100 // 转换单位为 0.1 毫秒

		// 构建日志前缀：自定义时间戳（6 位，单位 0.1ms）+ 节点所处状态 + 节点标识
        prefix := fmt.Sprintf("%06d %v ", time, string(topic))
        // 将前缀添加到原始字符串前
		format = prefix + format

		// 使用标准日志库输出
        log.Printf(format, a...)
    }
}
