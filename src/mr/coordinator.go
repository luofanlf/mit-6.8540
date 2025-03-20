package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
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
	workers            map[string]*Worker // workerId -> worker 指针
	nMap               int                //记录map任务数
	compeletedMap      int                //已完成map任务数
	nReduce            int                //记录reduce任务数
	compeletedReduce   int                //已完成reduce任务数
	isDone             bool               //是否完成任务
	AssignedMapTasks   MapTask
	mu                 sync.Mutex // 保护对共享数据的访问
	workerHeartbeatMap map[string]int64
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignMapTask(args *AskMapTaskArgs, reply *AskMapTaskReply) error {
	c.mu.Lock()
	c.workers[args.WorkerId] = &Worker{
		WorkerId: args.WorkerId,
		Status:   "running",
	}
	c.mu.Unlock()

	select {
	case task := <-c.mapTasksChan:
		task.TaskStatus = "IN_PROGRESS"
		c.mu.Lock()
		c.mapTasks[task.TaskId] = &task
		c.workers[args.WorkerId].MapTasks = append(c.workers[args.WorkerId].MapTasks, task.TaskId)
		c.mu.Unlock()

		log.Printf("assign map task: %v to worker: %v", task, args.WorkerId)
		reply.Acknowledged = true
		reply.Task = task
		reply.NReduce = c.nReduce
	default:
		log.Printf("no map task available")
		if c.compeletedMap == c.nMap {
			reply.Acknowledged = false
		} else {
			reply.Acknowledged = true
		}
	}
	return nil
}

func (c *Coordinator) HandleReportMapTask(arg *ReportMapTaskArgs, reply *ReportMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	//如果map任务成功执行，则将map任务数加1
	if arg.IsJobDone {
		taskId := arg.Task.TaskId
		//更新任务状态
		c.mapTasks[taskId].TaskStatus = "COMPLETED"
		log.Printf("report map: %v task success", taskId)
		c.compeletedMap++
	}

	log.Printf("Map tasks completed: %v/%v. Remaining: %v", c.compeletedMap, c.nMap,
		c.nMap-c.compeletedMap)
	//如果map任务数等于总map任务数，则将reduce任务数设置为总map任务数
	if c.compeletedMap == c.nMap {

		c.initReduceTask()
	}
	reply.Acknowledged = true
	return nil
}

// 初始化reduce任务
func (c *Coordinator) initReduceTask() {
	//重置currentid为reduce任务计数
	c.currentTaskId = 1
	for i := 0; i < c.nReduce; i++ {
		reduceTask := ReduceTask{
			TaskId:     c.currentTaskId,
			TaskStatus: "IDLE", // 添加初始状态
			Filenames:  []string{},
		}

		//通过reduceId来匹配存储map结果的缓存文件
		pattern := fmt.Sprintf("mr-*-%d", i)
		matches, err := filepath.Glob(pattern)
		if err != nil {
			log.Printf("Error matching files: %v", err)
			continue
		}
		reduceTask.Filenames = matches

		// 同时更新 reduceTasks map
		c.reduceTasks[reduceTask.TaskId] = &reduceTask
		c.reduceTasksChan <- reduceTask
		log.Printf("...............init reduce task: %v...........", reduceTask.TaskId)
		c.currentTaskId++
	}
	log.Printf("init reduce task success")
	log.Printf("length of reduce tasks: %v", len(c.reduceTasks))
}

func (c *Coordinator) AssignReduceTask(arg *AskForReduceTaskArgs, reply *AskForReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("ask for reduce task")

	select {
	case task := <-c.reduceTasksChan:
		task.TaskStatus = "IN_PROGRESS"
		c.workers[arg.WorkerId].ReduceTasks = append(c.workers[arg.WorkerId].ReduceTasks, task.TaskId)
		c.reduceTasks[task.TaskId] = &task

		log.Printf("assign reduce task: %v to worker: %v", task.TaskId, arg.WorkerId)
		reply.Task = task
		reply.Acknowledged = true
	default:
		log.Printf("no reduce task available")
		if c.compeletedReduce == c.nReduce {
			reply.Acknowledged = false
		} else {
			reply.Acknowledged = true
		}
	}
	return nil
}

func (c *Coordinator) HandleReportReduceTask(arg *ReportReduceTaskArgs, reply *ReportReduceTaskReply) error {
	// 如果reduce任务成功执行，则将reduce任务数加1
	if arg.IsJobDone {
		arg.Task.TaskStatus = "COMPLETED"
		c.reduceTasks[arg.Task.TaskId].TaskStatus = "COMPLETED"
		log.Printf("reduce task: %v success", arg.Task.TaskId)
		c.compeletedReduce++
	}
	log.Printf("........NO: %v REDUCE TASK DONE", c.compeletedReduce)
	// 如果reduce任务数等于总reduce任务数，则标记任务完成
	if c.compeletedReduce == c.nReduce {
		c.isDone = true
	}

	reply.Acknowledged = true
	return nil
}

func (c *Coordinator) AnswerPingCoordinator(arg *PingCoordinatorArgs, reply *PingCoordinatorReply) error {
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
	if c.isDone {
		for taskid, task := range c.mapTasks {
			log.Printf("map task: %v, status: %v", taskid, task.TaskStatus)
		}
		for taskid, task := range c.reduceTasks {
			log.Printf("reduce task: %v, status: %v", taskid, task.TaskStatus)
		}
		log.Printf("tasks are done,coordinator is exiting.........")
	}
	// Your code here.

	return c.isDone
}

func (c *Coordinator) AnswerHeartbeat(arg *HeartbeatArgs, reply *HeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerId := arg.WorkerId
	currentTime := time.Now().Unix()

	// 检查 worker 是否存在，如果不存在则创建
	// if _, exists := c.workers[workerId]; !exists {
	// 	c.workers[workerId] = &Worker{
	// 		WorkerId:    workerId,
	// 		Status:      "running",
	// 		MapTasks:    []int{},
	// 		ReduceTasks: []int{},
	// 	}
	// } else {
	// 	// 更新已存在 worker 的心跳时间
	// 	c.workers[workerId].Heartbeat = currentTime
	// }
	c.workerHeartbeatMap[workerId] = currentTime

	log.Printf("worker %v heartbeat at %v", workerId, c.workerHeartbeatMap[workerId])
	reply.Acknowledged = true
	return nil
}

func (c *Coordinator) checkWorkerStatus() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, worker := range c.workers {
		log.Printf("??????????worker %v ", worker)
		if worker.Status == "running" {
			log.Printf("worker %v distance of last heart beat: %v,timenow: %v,timestamp: %v",
				worker.WorkerId,
				time.Now().Unix()-c.workerHeartbeatMap[worker.WorkerId],
				time.Now().Unix(),
				c.workerHeartbeatMap[worker.WorkerId])

			if time.Now().Unix()-c.workerHeartbeatMap[worker.WorkerId] > 5 {
				log.Printf("worker %v is becoming dead, reassigning its tasks", worker.WorkerId)
				worker.Status = "dead"

				// 重新分配 map 任务
				for _, taskId := range worker.MapTasks {
					if task, exists := c.mapTasks[taskId]; exists && task.TaskStatus == "IN_PROGRESS" {
						log.Printf("Reassigning map task %v from dead worker %v", taskId, worker.WorkerId)
						task.TaskStatus = "IDLE"
						c.mapTasksChan <- *task
					}
				}

				// 重新分配 reduce 任务
				for _, taskId := range worker.ReduceTasks {
					if task, exists := c.reduceTasks[taskId]; exists && task.TaskStatus == "IN_PROGRESS" {
						log.Printf("Reassigning reduce task %v from dead worker %v", taskId, worker.WorkerId)
						task.TaskStatus = "IDLE"
						c.reduceTasksChan <- *task
					}
				}

				// 清空该 worker 的任务列表
				worker.MapTasks = []int{}
				worker.ReduceTasks = []int{}

				log.Printf("worker %v is dead, all tasks reassigned", worker.WorkerId)
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
		mapTasksChan:       make(chan MapTask, len(files)),
		reduceTasksChan:    make(chan ReduceTask, nReduce),
		currentTaskId:      1,
		nMap:               len(files),
		compeletedMap:      0,
		nReduce:            nReduce,
		compeletedReduce:   0,
		mapTasks:           make(map[int]*MapTask),
		reduceTasks:        make(map[int]*ReduceTask),
		workers:            make(map[string]*Worker),
		workerHeartbeatMap: make(map[string]int64),
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
		log.Printf("start check worker status")
		for {
			time.Sleep(1 * time.Second)
			c.checkWorkerStatus()
		}
	}()
	c.server()

	return &c
}
