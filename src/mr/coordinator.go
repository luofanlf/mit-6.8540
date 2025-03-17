package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

type MapTask struct {
	TaskId     int
	Filename   string
	TaskStatus string
}

type ReduceTask struct {
	TaskId     int
	Filenames  []string
	TaskStatus string
}

type Worker struct {
	WorkerId    string
	MapTasks    []int
	ReduceTasks []int
	Status      string
	Heartbeat   int64
}

type Coordinator struct {
	// Your definitions here.
	mapTasksChan    chan MapTask
	reduceTasksChan chan ReduceTask
	currentTaskId   int //用于生成taskid
	// 任务状态跟踪
	mapTasks    map[int]*MapTask    // taskId -> task 指针
	reduceTasks map[int]*ReduceTask // taskId -> task 指针

	// worker 状态跟踪
	workers          map[string]*Worker // workerId -> worker 指针
	nMap             int                //记录map任务数
	compeletedMap    int                //已完成map任务数
	nReduce          int                //记录reduce任务数
	compeletedReduce int                //已完成reduce任务数
	isDone           bool               //是否完成任务
	AssignedMapTasks MapTask
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AskForMapTask(args *AskMapTaskArgs, reply *AskMapTaskReply) error {
	// 添加短暂延迟
	time.Sleep(1000 * time.Millisecond)

	//将worker记录在案
	c.workers[args.WorkerId] = &Worker{
		WorkerId: args.WorkerId,
		Status:   "running",
	}

	//派发任务
	select {
	case task := <-c.mapTasksChan:
		task.TaskStatus = "IN_PROGRESS"
		//存储task状态
		c.mapTasks[task.TaskId] = &task
		log.Printf("assign map task: %v to worker: %v", task, args.WorkerId)
		reply.Acknowledged = true
		reply.Task = task
		reply.NReduce = c.nReduce
	default:
		log.Printf("no map task available")
		reply.Acknowledged = false
	}
	return nil
}

func (c *Coordinator) ReportMapTask(arg *ReportMapTaskArgs, reply *ReportMapTaskReply) error {
	//如果map任务成功执行，则将map任务数加1
	if arg.IsJobDone {
		taskId := arg.Task.TaskId
		//更新任务状态
		c.mapTasks[taskId].TaskStatus = "COMPLETED"
		log.Printf("report map: %v task success", taskId)
		c.compeletedMap++
	}

	//如果map任务数等于总map任务数，则将reduce任务数设置为总map任务数
	if c.compeletedMap == c.nMap {

		c.initReduceTask()
	}
	return nil
}

// 初始化reduce任务
func (c *Coordinator) initReduceTask() {
	//重置currentid为reduce任务计数
	c.currentTaskId = 1
	for i := 0; i < c.nReduce; i++ {
		reduceTask := ReduceTask{
			TaskId:    c.currentTaskId,
			Filenames: []string{},
		}

		//通过reduceId来匹配存储map结果的缓存文件
		pattern := fmt.Sprintf("mr-*-%d", i)
		matches, err := filepath.Glob(pattern)
		if err != nil {
			log.Printf("Error matching files: %v", err)
			continue
		}
		reduceTask.Filenames = matches
		c.reduceTasksChan <- reduceTask
		c.currentTaskId++
	}
	log.Printf("init reduce task success")
}

func (c *Coordinator) AskForReduceTask(arg *AskForReduceTaskArgs, reply *AskForReduceTaskReply) error {
	// 添加短暂延迟
	time.Sleep(1000 * time.Millisecond)

	//派发任务
	select {
	case task := <-c.reduceTasksChan:
		log.Printf("assign reduce task: %v to worker: %v", task, arg.WorkerId)
		//存储任务状态
		task.TaskStatus = "IN_PROGRESS"
		c.reduceTasks[task.TaskId] = &task
		reply.Acknowledged = true
		reply.Task = task

	default:
		log.Printf("no reduce task available")
		reply.Acknowledged = false
	}
	return nil
}

func (c *Coordinator) ReportReduceTask(arg *ReportReduceTaskArgs, reply *ReportReduceTaskReply) error {
	// 如果reduce任务成功执行，则将reduce任务数加1
	if arg.IsJobDone {
		arg.Task.TaskStatus = "COMPLETED"
		c.reduceTasks[arg.Task.TaskId].TaskStatus = "COMPLETED"
		log.Printf("reduce task: %v success", arg.Task.TaskId)
		c.compeletedReduce++
	}

	// 如果reduce任务数等于总reduce任务数，则标记任务完成
	if c.compeletedReduce == c.nReduce {
		c.isDone = true
	}

	reply.Acknowledged = true
	return nil
}

func (c *Coordinator) PingCoordinator(arg *PingCoordinatorArgs, reply *PingCoordinatorReply) error {
	reply.Acknowledged = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	//注册rpc服务，采用http协议通信，并且监听sockname端口
	fmt.Println("start server")
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

	// Your code here.
	log.Printf("tasks are done,coordinator is exiting.........")
	return c.isDone
}

func (c *Coordinator) Heartbeat(arg *HeartbeatArgs, reply *HeartbeatReply) error {
	// workerId := arg.WorkerId
	// for i := range c.workers {
	// 	if c.workers[i].WorkerId == worker.WorkerId {
	// 		c.workers[i].Heartbeat = time.Now().Unix()
	// 		break
	// 	}
	// }
	// reply.Acknowledged = true
	return nil
}

func (c *Coordinator) checkWorkerStatus() {
	for _, worker := range c.workers {
		if worker.Status == "running" {
			if time.Now().Unix()-worker.Heartbeat > 5 {
				worker.Status = "dead"
				log.Printf("worker %v is dead", worker.WorkerId)
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//初始化coordinator
	c := Coordinator{
		mapTasksChan:     make(chan MapTask, len(files)),
		reduceTasksChan:  make(chan ReduceTask, nReduce),
		currentTaskId:    1,
		nMap:             len(files),
		compeletedMap:    0,
		nReduce:          nReduce,
		compeletedReduce: 0,
	}

	//初始化map任务,把file分割成多个map任务塞进channel
	for _, filename := range files {
		c.mapTasksChan <- MapTask{
			TaskId:     c.currentTaskId,
			Filename:   filename,
			TaskStatus: "IDLE",
		}
		c.currentTaskId++
	}

	// Your code here.

	go func() {
		for {
			time.Sleep(5 * time.Second)
			c.checkWorkerStatus()
		}
	}()
	c.server()

	return &c
}
