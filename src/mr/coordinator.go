package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	STATUSMAP = iota
	STATUSREDUCE
)

type Coordinator struct {
	// Your definitions here.
	Files      []string
	MapJobs    []Job
	ReduceJobs []Job
	NReduce    int
	Status     int
	mu         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) printAllJob() {
	// 打印结构体字段逐个值
	fmt.Println("------------------")
	for index, job := range c.MapJobs {
		fmt.Println(index, ": ", job.Statu)
	}
	for index, job := range c.ReduceJobs {
		fmt.Println(index, ": ", job.Statu)
	}
	fmt.Println("------------------")
}
func (c *Coordinator) GotJob(args *JobRequest, reply *Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.printAllJob()
	var jobs []Job
	switch c.Status {
	case STATUSMAP:
		jobs = c.MapJobs
	case STATUSREDUCE:
		jobs = c.ReduceJobs
	default:
		return nil
	}

	// 查找未分配的任务
	for index := range jobs {
		jobPtr := &jobs[index]
		if jobPtr.Statu == STATUSUNDISTRIBUTED {
			jobPtr.Statu = STATUSDISTRIBUTED
			jobPtr.DistributeTime = time.Now()
			*reply = *jobPtr
			log.Println("distribute job id=", jobPtr.Id)
			return nil
		}
	}

	// 查找超时未完成的任务
	for index := range jobs {
		jobPtr := &jobs[index]
		if time.Since(jobPtr.DistributeTime).Seconds() > 10 && jobPtr.Statu != STATUSJOBFINISHED {
			jobPtr.Statu = STATUSDISTRIBUTED
			jobPtr.DistributeTime = time.Now()
			*reply = *jobPtr
			return nil
		}
	}

	return nil
}

func (c *Coordinator) ReportJob(args *JobFeedback, reply *Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var jobs []Job
	switch args.Type {
	case "map":
		jobs = c.MapJobs
	case "reduce":
		jobs = c.ReduceJobs
	default:
		return nil
	}

	for index := range jobs {
		jobPtr := &jobs[index]

		if jobPtr.Id == args.Id {
			*reply = *jobPtr
			if jobPtr.Statu == STATUSJOBFINISHED {
				break
			} else {

				handleFiles(args)
				jobPtr.Statu = args.Statu
				break
			}
		}
	}
	return nil
}

func handleFiles(args *JobFeedback) {
	if args.Type == "map" {
		filenamesTemp := generateIntermidiateFileNamesTimeStamp(args.Job, args.TimeStamp)
		filenames := generateIntermidiateFileNames(args.Job)
		for index := range args.NReduce {
			os.Rename(filenamesTemp[index], filenames[index])
		}
	} else {
		filenamesTemp := generateOutputFileNamesTimeStamp(args.Job, args.TimeStamp)
		filenames := generateOutputFileNames(args.Job)
		os.Rename(filenamesTemp, filenames)
	}

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
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
	for _, job := range c.MapJobs {
		if job.Statu != STATUSJOBFINISHED {
			return ret
		}
	}
	c.Status = STATUSREDUCE
	for _, job := range c.ReduceJobs {
		if job.Statu != STATUSJOBFINISHED {
			return ret
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// c := Coordinator{}

	// Your code here.
	c := Coordinator{
		Files:   files,
		NReduce: nReduce,
		Status:  STATUSMAP, // 假设初始状态为MAP
	}

	// 初始化 MapJobs (假设有一个文件对应一个 Map 任务)
	for i, file := range files {
		job := NewJob(i, "map", file, nReduce)
		c.MapJobs = append(c.MapJobs, job)
	}

	// 初始化 ReduceJobs (创建 nReduce 个 Reduce 任务)
	for i := 0; i < nReduce; i++ {
		job := NewJob(i, "reduce", "", nReduce)
		c.ReduceJobs = append(c.ReduceJobs, job)
	}

	c.server()
	return &c
}
