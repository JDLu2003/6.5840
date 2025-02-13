package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// CallExample()
	// Your worker implementation here.
	for {
		job := CallGotJob()
		switch job.Type {
		case "map":
			doMapJob(mapf, job)
		case "reduce":
			doReduceJob(reducef, job)
		default:

			log.Println("got unkonwn job", time.Now())
			time.Sleep(2 * time.Second)
			continue
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func generateIntermidiateFileNamesTimeStamp(job Job, timeStamp int64) []string {
	filenames := make([]string, job.NReduce)
	for index := range job.NReduce {
		filenames[index] = fmt.Sprintf("temp-%d-%d-%d", job.Id, index, timeStamp)
	}
	return filenames
}
func generateIntermidiateFileNames(job Job) []string {
	filenames := make([]string, job.NReduce)
	for index := range job.NReduce {
		filenames[index] = fmt.Sprintf("mr-%d-%d", job.Id, index)
	}
	return filenames
}

func saveIntermidiate(data []KeyValue, job Job) error {
	timeStamp := time.Now().UnixNano()
	nReduce := job.NReduce
	filenames := generateIntermidiateFileNamesTimeStamp(job, timeStamp)
	files := make([]*os.File, nReduce)
	for index := range nReduce {
		file, err := os.OpenFile(filenames[index], os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			log.Println("Error opening file:", err)
			return err
		}
		files[index] = file
		defer files[index].Close()
	}
	for index := range nReduce {
		file, err := os.OpenFile(filenames[index], os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			log.Println("Error opening file:", err)
			return err
		}
		files[index] = file
		defer files[index].Close()
	}

	// 创建 JSON 编码器
	encs := make([]*json.Encoder, nReduce)
	for index := range nReduce {
		encs[index] = json.NewEncoder(files[index])
	}

	// 逐行写入数据
	for _, kv := range data {
		index := ihash(kv.Key) % nReduce
		err := encs[index].Encode(&kv)
		if err != nil {
			return err
		}
	}
	CallReportJob(job, timeStamp)
	for _, file := range filenames {
		os.Remove(file)
	}
	return nil
}

func readFromFile(filename string) ([]KeyValue, error) {
	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 创建 JSON 解码器
	dec := json.NewDecoder(file)

	var kva []KeyValue

	// 逐行读取数据
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			// 如果读到文件末尾或其他错误，退出循环
			break
		}
		kva = append(kva, kv)
	}

	return kva, nil
}

func doMapJob(mapf func(string, string) []KeyValue, job Job) {
	intermidiate := []KeyValue{}
	file, err := os.Open(job.FileNmae)
	if err != nil {
		log.Fatalf("canot open %v", job.FileNmae)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("canot open %v", job.FileNmae)
	}
	file.Close()
	kva := mapf(job.FileNmae, string(content))
	intermidiate = append(intermidiate, kva...)
	// should save intermidia to mr-X-Y, X is code of map
	// and Y is code of reduce.
	// we should use name "temp-X-Y" to save temp file and
	// change its name to "mr-X-Y" atomly.
	// the intermidiate save to temp file in JSON style
	saveIntermidiate(intermidiate, job)
}

func doReduceJob(reducef func(string, []string) string, job Job) {
	files, err := filepath.Glob(fmt.Sprintf("mr-*-%d", job.Id))
	if err != nil {
		log.Println("Error finding files:", err)
		return
	}

	var allKeyValues []KeyValue

	// 遍历符合条件的文件
	for _, fileName := range files {
		kva, err := readFromFile(fileName)
		if err != nil {
			log.Println("failed to read ", fileName)
			return
		}
		allKeyValues = append(allKeyValues, kva...)
	}
	sort.Sort(ByKey(allKeyValues))
	timeStamp := time.Now().UnixNano()
	oname := generateOutputFileNamesTimeStamp(job, timeStamp)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(allKeyValues) {
		j := i + 1
		for j < len(allKeyValues) && allKeyValues[j].Key == allKeyValues[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, allKeyValues[k].Value)
		}
		output := reducef(allKeyValues[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", allKeyValues[i].Key, output)

		i = j
	}
	ofile.Close()
	CallReportJob(job, timeStamp)
	os.Remove(oname)
}

func generateOutputFileNamesTimeStamp(job Job, timeStamp int64) string {
	name := fmt.Sprintf("temp-out-%d-%d", job.Id, timeStamp)
	return name
}
func generateOutputFileNames(job Job) string {
	name := fmt.Sprintf("mr-out-%d", job.Id)
	return name
}

// worker从coordinator获得任务
func CallGotJob() Job {
	var args struct{}
	reply := Job{}
	ok := call("Coordinator.GotJob", &args, &reply)
	if ok {
		log.Print("got job ")
		log.Println(reply.Type, reply.FileNmae)
	} else {
		log.Println("got job error")
	}
	return reply
}

// worker向coordinator报告完成任务
func CallReportJob(job Job, timeStamp int64) {
	job.Statu = STATUSJOBFINISHED
	args := JobFeedback{
		Job:        job,
		TimeStamp:  timeStamp,
		IsFinished: true,
	}
	reply := job
	ok := call("Coordinator.ReportJob", &args, &reply)
	if ok {
		log.Println("succeed report", job.Type, job.Id)
	} else {
		log.Println("call failed !")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		log.Printf("reply.Y %v\n", reply.Y)
	} else {
		log.Printf("call failed!\n")
	}
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

	log.Println(err)
	return false
}
