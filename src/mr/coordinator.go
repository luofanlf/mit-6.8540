package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type MapTask struct {
	TaskId   int
	WorkerId string
	Filename string
}

type ReduceTask struct {
	TaskId   int
	WorkerId string
}

type Coordinator struct {
	// Your definitions here.
	mapTasks      chan MapTask
	reduceTasks   chan ReduceTask
	currentTaskId int //用于生成taskid
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskForTask(args *AskTaskArgs, reply *AskTaskReply) error {
	reply.TaskType = "map"
	reply.TaskId = 0
	reply.FileName = "test.txt"
	reply.NumOtherPhase = 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	//注册rpc服务，采用http协议通信，并且监听sockname端口
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//初始化coordinator
	c := Coordinator{
		mapTasks:      make(chan MapTask, len(files)),
		reduceTasks:   make(chan ReduceTask, nReduce),
		currentTaskId: 1,
	}

	//初始化map任务
	for _, filename := range files {
		c.mapTasks <- MapTask{
			TaskId:   c.currentTaskId,
			WorkerId: "",
			Filename: filename,
		}
		c.currentTaskId++
	}

	// Your code here.

	c.server()
	return &c
}
