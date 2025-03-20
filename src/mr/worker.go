package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
//
// map函数返回的键值对
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// 将哈希值映射到n个桶中
func ihash(key string, n int) int {
	//创建一个新的hash函数实例
	h := fnv.New32a()
	//将key写入hash函数
	h.Write([]byte(key))
	//返回hash值
	return int(h.Sum32()&0x7fffffff) % n
}

// main/mrworker.go calls this function.
func MakeWorker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	//随机生成一个workerid
	workerId := uuid.New().String()
	log.Printf("successful init worker,id: %v", workerId)

	// Your worker implementation here.
	//心跳机制，用于检验worker故障
	go func() {
		for {
			Heartbeat(workerId)
			log.Printf("worker %v ping coordinator", workerId)
			time.Sleep(2 * time.Second)
		}
	}()
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
	// log.Printf("map task success, intermediate: %v", intermediate)

	//reduce任务
	for {
		log.Printf("reduce task")
		task, acknowledged := AskForReduceTask(workerId)
		filenames := task.Filenames
		taskId := task.TaskId

		log.Printf("reduce task: %v, filenames: %v", taskId, filenames)
		if acknowledged {
			if task.TaskId == 0 {
				// 没有新任务，但还有未完成的 map 任务，等待后重试
				time.Sleep(1 * time.Second)
				continue
			}
			log.Printf("successful fetch reduce task:% v", taskId)
			var intermediate []KeyValue
			//读取reduce任务对应的文件中的健值对
			for _, filename := range filenames {
				log.Printf("read reduce task file: %v", filename)
				f, err := os.Open(filename)
				if err != nil {
					return
				}
				dec := json.NewDecoder(f)

				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			//执行reduce任务
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%v", taskId)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			log.Printf("intermediate: %v", len(intermediate))
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()

			acknowledged = ReportReduceTask(workerId, task, true)
			if !acknowledged {
				log.Printf("report map task failed")
				break
			}
		} else {
			break
		}
	}

	// 等待所有reduce任务完成，确认coordinator是否退出，如果coordinator退出则worker也可以退出
	for {
		time.Sleep(1 * time.Second)
		acknowledged := pingCoordinator(workerId)
		if !acknowledged {
			log.Printf("coordinator not responding, exiting")
			break
		}
	}

}

// rpc方法：向coordinator请求任务
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

// rpc方法：向coordinator请求reduce任务
func AskForReduceTask(workerId string) (task ReduceTask, acknowledged bool) {

	//请求参数
	args := AskForReduceTaskArgs{
		WorkerId: workerId,
	}
	//响应参数
	reply := AskForReduceTaskReply{}

	//调用请求reduce任务rpc
	ok := call("Coordinator.AssignReduceTask", &args, &reply)
	if !ok {
		log.Printf("ask for reduce task failed")
		return ReduceTask{}, false
	}

	//根据任务类型处理任务
	return reply.Task, reply.Acknowledged
}

func ReportReduceTask(workerId string, task ReduceTask, isJobDone bool) (acknowledged bool) {

	//请求参数
	args := ReportReduceTaskArgs{}
	args.WorkerId = workerId
	args.Task = task
	args.IsJobDone = isJobDone

	//响应参数
	reply := ReportReduceTaskReply{}

	//调用报告reduce任务rpc
	ok := call("Coordinator.HandleReportReduceTask", &args, &reply)
	if !ok {
		log.Printf("report reduce task failed")
		return false
	}

	return reply.Acknowledged

}

func pingCoordinator(workerId string) (acknowledged bool) {
	args := PingCoordinatorArgs{
		WorkerId: workerId,
	}
	reply := PingCoordinatorReply{}
	ok := call("Coordinator.AnswerPingCoordinator", &args, &reply)
	if !ok {
		log.Printf("ping coordinator failed")
		return false
	}
	return reply.Acknowledged
}

func Heartbeat(workerId string) (acknowledged bool) {

	args := HeartbeatArgs{
		WorkerId: workerId,
	}
	reply := HeartbeatReply{}
	ok := call("Coordinator.AnswerHeartbeat", &args, &reply)
	if !ok {
		log.Printf("heartbeat failed")
		return false
	}
	return reply.Acknowledged
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
