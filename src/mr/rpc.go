package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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

const (
	//任务未分配状态
	STATUSUNDISTRIBUTED = iota
	//任务已分配状态
	STATUSDISTRIBUTED
	//任务完成状态
	STATUSJOBFINISHED
)

// Add your RPC definitions here.
type Job struct {
	Type           string
	FileNmae       string
	DistributeTime time.Time
	Statu          int
	NReduce        int
	Id             int
}

func NewJob(id int, jobType string, fileName string, nReduce int) Job {
	return Job{
		Type:           jobType,
		FileNmae:       fileName,
		DistributeTime: time.Time{},
		Statu:          STATUSUNDISTRIBUTED,
		NReduce:        nReduce,
		Id:             id,
	}
}

type JobRequest struct{}

type JobFeedback struct {
	Job
	IsFinished bool
	TimeStamp  int64
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
