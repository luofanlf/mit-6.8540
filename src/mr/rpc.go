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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// 请求任务参数
type AskTaskArgs struct {
	WorkerId string
}

// 请求任务回复
type AskTaskReply struct {
	TaskType      string //可以是map,reduce
	TaskId        int    //任务id
	FileName      string //map任务的文件名
	NumOtherPhase int    //reduce任务的数量
}

// Add your RPC definitions here.

type ReoportTaskArgs struct {
	TaskType     string
	TaskId       int
	WorkerId     string
	IsCompeleted bool //是否完成任务
}

type ReportTaskReply struct {
	Acknowledged bool //coordinator是否收到任务报告
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
