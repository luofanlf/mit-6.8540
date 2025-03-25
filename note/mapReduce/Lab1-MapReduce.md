# Lab1-MapReduce

这是mit-6.5840 distributed system的lab1，lab1的基本内容就是需要复现google的mapReduce模型，论文链接如下：http://nil.csail.mit.edu/6.5840/2024/papers/mapreduce.pdf

lab要求：http://nil.csail.mit.edu/6.5840/2024/labs/lab-mr.html

## MapReduce

mapreduce是一个模型用于处理大量数据，可以理解为数据处理的框架，只需要改变任务执行的算法而不需要关心底层的并行计算，容错，数据分布等细节

![./mapReduce.png](/Users/luofan/笔记/mit-6.8540/mapReduce/mapReduce.png)



接下来以wordCount为例来介绍mapReduce的基本流程：

1. 首先输入的数据会被自动分割成m个数据片段，例如一篇文章被分为多段，mapReduce的最终任务就是统计文章中每个word的出现次数。
2. mapReduce模型主要由一个coordinator和多个worker构成，在数据被分割后，coordinator会把M个map任务分割给空闲的worker，这一阶段需要输出key/value pair。例：“hello world”，“hello map reduce”，“hello hello luofan”分别分配给worker1，2，3，worker1输出（hello，“1”），（world，“1”），worker2输出（hello，“1”），（map，“1”），（reduce，“1”）worker3输出（hello，“1”），（hello，“1”），（luofan，“1”）
3. map任务得到的key/value pair被分配到R个桶中（hash（key）mod R），R为reduce任务以及最终输出文件数量，并将kv pair存储在worker的本地disk中，在此lab中，将kv对存储在文件中
4. map任务执行完以及intermediate存储到文件后，空闲的worker执行由coordinator分配的reduce任务，统计每个词的出现次数将输出结果存储到R个文件中



## Lab思路

我在mac上完成了整个lab，对于windows可以采用虚拟机或者wsl的方案

首先从官方clone lab代码

```
$ git clone git://g.csail.mit.edu/6.5840-golabs-2024 6.5840
$ cd 6.5840
$ ls
Makefile src
$
```

接着尝试运行mrsequential，这个相当于单一个worker执行完了map和reduce的工作

```
$ cd ~/6.5840
$ cd src/main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0
A 509
ABOUT 2
ACT 8
...
```

接下来看一下项目的代码结构。

**mrapps**下的代码实现了各个测试用例中的map和reduce函数

**main**下的mrcoodinatior.go和mrworker.go两个文件，这两个文件用于启动coordinator，worker和加载plugin，pg开头的txt文件是这个lab中的输入文件

**mr**下的coordinator.go,rpc.go以及worker.go三个文件是我们需要完成的内容

在运行前需要go build -buildmode=plugin来编译插件以供后续导入。

整个实验流程中，基本上是先启动coordinator

```
$ go build -buildmode=plugin ../mrapps/wc.go
$ go run mrcoordinator.go pg-*.txt
```

然后在多个终端中启动worker开始mapreduce

```
$ go run mrworker.go wc.so
```



我对于这个lab的实现是将coordinator作为一个server，将所有task，worker，以及task channel等所有数据都维护在coordinator中。我对于任务分配的思路是，在coordianator启动时，将所有任务塞进task channel中，让work来请求任务，由于golang的channel天然的并发安全所以不需要考虑race的问题

#### 定义coordinator中的一些struct

coordinator.go

```go
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
```

#### 初始化coordinator

coordinator.go

```go
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
	return &c
}
```

#### 初始化worker，请求mapTask

官方的lab guidance中提到可以从worker向coordinator发起一个maptask请求入手，worker和coordinator之间通过rpc通信

##### 在rpc.go中定义请求参数和响应参数

```go
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
```

##### worker.go中调用rpc函数，coordinator实现函数

原本仓库中的代码已经实现了rpc调用，只要通过worker.go的call这个函数就可以调用coordinator中的函数

worker.go

```go
func AskForMapTask(workerId string) (task MapTask, acknowledged bool, nReduce int) {

	//请求参数
	args := AskMapTaskArgs{}
	args.WorkerId = workerId

	//响应参数
	reply := AskMapTaskReply{}

	//调用请求任务rpc
	ok := call("Coordinator.AssignMapTask", &args, &reply)
	if !ok {
		fmt.Println("ask for map task failed")
		return MapTask{}, false, 0
	}

	//根据任务类型处理任务
	return reply.Task, reply.Acknowledged, reply.NReduce
}

```

Coordinator.go

```go
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
```

#### worker启动后持续请求map任务

uuid生成workid作为唯一标识，在调用rpc时传递给coordinator

worker.go

```go
func MakeWorker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	//随机生成一个workerid
	workerId := uuid.New().String()
	log.Printf("successful init worker,id: %v", workerId)

	// uncomment to send the Example RPC to the coordinator.
	//map任务
	for {
		task, acknowledged, nReduce := AskForMapTask(workerId)
		if acknowledged {
			if task.TaskId == 0 {
				// 没有新任务，但还有未完成的 map 任务，等待后重试
				time.Sleep(1 * time.Second)
				continue
			}
			filename := task.Filename
			taskId := task.TaskId
			log.Printf("fetch maptask success, taskid: %v", taskId)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			//将健值对写入缓存
			kva := mapf(filename, string(content))
			for _, kv := range kva {
				//将健值对映射到n个桶中
				bucket := ihash(kv.Key, nReduce)
				//生成文件名
				fileName := fmt.Sprintf("mr-tmp-%v-%v", taskId, bucket)

				// 打开文件进行追加，如果文件不存在则创建
				f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Fatalf("failed to open file: %v", err)
				}

				enc := json.NewEncoder(f)
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatalf("failed to encode to JSON: %v", err)
				}

				f.Close()

			}

			log.Printf("map task %v successfully done ", taskId)

			//返回任务执行response
			acknowledged = ReportMapTask(workerId, task, true)
			if !acknowledged {
				log.Printf("report map task failed")
				break
			}
		} else {
			// 所有 map 任务都完成了，进入 reduce 阶段
			break
		}
	}
```

#### reportMapTask

worker完成了map任务之后，需要向coordinator报告任务完成情况，coordinator记录完成任务数量，如果所有的maptask都被完成，则将输出的所有intermediate文件初始化为reduce任务,并且先完成了maptask的worker也需要等待，到所有的maptask都被完成以后才可以向coordinator请求reducetasl

Rpc.go

```go
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
```

worker.go

```go
// rpc方法：向coordinator报告任务完成情况
func ReportMapTask(workerId string, task MapTask, isJobDone bool) (acknowledged bool) {

	//请求参数
	args := ReportMapTaskArgs{}
	args.WorkerId = workerId
	args.Task = task
	args.IsJobDone = isJobDone

	//响应参数
	reply := ReportMapTaskReply{}

	//调用报告任务rpc
	ok := call("Coordinator.HandleReportMapTask", &args, &reply)
	if !ok {
		log.Printf("report map task failed")
		return false
	}
	reply.Acknowledged = true
	return reply.Acknowledged
}

```

Coordinator.go

```go
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
```

#### 初始化ReduceTask

map函数将intermediate以mr-tmp-{{mapTaskId}}-{{bucket number}}的形式存储，coordinator在所有maptask完成后初始化reduceTask

coordinator.go

```go
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
```

#### 请求reduce任务，输出结果

接来下worker请求reduce任务，实现方式与请求map任务类似，至此已经可以成功完成整个mapreduce的流程

#### HeartBeat Mechinims

测试用例中有一个crash test,因此我们实现心跳机制，在worker启动后，起一个goroutine每隔一段时间就ping coordinator，在coordinator端起一个goroutine接收心跳，如果超出一段时间没有收到来自worker的消息就将其标记为故障，检索数据结构中存储的该worker对应的task下taskStatus是“IN_PROGRESS”的，将其状态设置为idle重新塞回channel中供仍然存活的worker处理

worker.go

```go
go func() {
		for {
			Heartbeat(workerId)
			log.Printf("worker %v ping coordinator", workerId)
			time.Sleep(2 * time.Second)
		}
	}()
```

coordinator.go

```go
func (c *Coordinator) AnswerHeartbeat(arg *HeartbeatArgs, reply *HeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerId := arg.WorkerId
	currentTime := time.Now().Unix()

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


go func() {
		log.Printf("start check worker status")
		for {
			time.Sleep(1 * time.Second)
			c.checkWorkerStatus()
		}
	}()
```

#### 运行测试脚本

```
$ bash test-mr.sh
```

至此完成lab1的所有内容，亲测可以通过mit给的所有测试用例，因为本人也是第一次接触并发编程，如果有什么不正确的地方欢迎指正交流

github地址：https://github.com/luofanlf/mit-6.8540