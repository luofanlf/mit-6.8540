package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// 请求map任务参数
type AskMapTaskArgs struct {
	WorkerId string
}

// 请求map任务回复
type AskMapTaskReply struct {
	Acknowledged bool
	Task         MapTask
	NReduce      int
}

// 报告map任务参数
type ReportMapTaskArgs struct {
	WorkerId  string
	Task      MapTask
	IsJobDone bool
}

// 报告map任务回复
type ReportMapTaskReply struct {
	Acknowledged bool
}

// 请求reduce任务参数
type AskForReduceTaskArgs struct {
	WorkerId string
}

// 请求reduce任务响应参数
type AskForReduceTaskReply struct {
	WorkerId     string
	Acknowledged bool
	Task         ReduceTask
}

// 报告reduce任务参数
type ReportReduceTaskArgs struct {
	WorkerId  string
	Task      ReduceTask
	IsJobDone bool
}

// 报告reduce任务响应参数
type ReportReduceTaskReply struct {
	Acknowledged bool
}

// ping coordinator请求参数
type PingCoordinatorArgs struct {
	WorkerId string
}

// ping coordinator响应参数
type PingCoordinatorReply struct {
	Acknowledged bool
}

// 心跳检测请求参数
type HeartbeatArgs struct {
	WorkerId  string
	Timestamp int64
}

// 心跳检测响应参数
type HeartbeatReply struct {
	Acknowledged bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
// 创建一个sockeet（可以理解为对讲机）以实现worker和coordinator之间的通信
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
