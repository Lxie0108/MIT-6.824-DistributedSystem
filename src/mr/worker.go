package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//send an RPC to master asking for a task.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		reply, error := ReqTask()
		if error != nil {
			continue
		}
		if reply.TaskType == "" {
			continue
		}
		switch reply.TaskType {
		case "map":
			Map(&reply, mapf)
			ChangeTaskState(reply.TaskType, reply.TaskId)
			time.Sleep(time.Millisecond)
		case "reduce":
			Reduce(&reply, reducef)
			ChangeTaskState(reply.TaskType, reply.TaskId)
			time.Sleep(time.Millisecond)
		}
	}

}

//workers ask for a task
func ReqTask() (ReqReply, error) {
	args := ReqArgs{}
	reply := ReqReply{}
	ok := call("Master.ReqTask", &args, &reply)
	if !ok {
		return reply, errors.New("no response")
	}
	//fmt.Printf("Reached ReqTask")
	return reply, nil
}

//workers do map task
func Map(reply *ReqReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	defer file.Close()
	//file.Close()
	//fmt.Printf("reached Map")
	//send file to mapf and get the key-value array
	kva := mapf(reply.Filename, string(content))
	//do partition
	res := make(map[int][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		v := ihash(kv.Key) % reply.NReduce
		res[v] = append(res[v], kv)
	}

	//then, write to temp file
	for i := 0; i < reply.NReduce; i++ {
		tempFilename := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i) // mr-X-Y, x is the map task number and y is the reduce task number
		tempFile, err := os.Create(tempFilename)
		if err != nil {
			log.Fatalf("error")
		}
		//use Go's encoding/json to store in file
		dec := json.NewEncoder(tempFile)
		for _, kv := range res[i] {
			dec.Encode(&kv)
		}
	}
}

//workers do reduce task
func Reduce(reply *ReqReply, reducef func(string, []string) string) {
	//read temp files using Json
	maps := map[string][]string{}
	for i := 0; i < reply.NMap; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("Open " + filename + " failed")
			return
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
		//file.Close()
	}
	//then sort
	keys := []string{}
	for key, _ := range maps {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	//finally write to mr-out
	output, _ := os.Create("mr-out-" + strconv.Itoa(reply.TaskId))
	defer output.Close()
	for _, key := range keys {
		cnt := reducef(key, maps[key])
		output.WriteString(fmt.Sprintf("%v %v\n", key, cnt))
	}
	//output.Close()
}

func ChangeTaskState(TaskType string, TaskId int) {
	args := ChangeTaskStateArgs{
		TaskType: TaskType,
		TaskId:   TaskId,
	}
	reply := ChangeTaskStateReply{}
	call("Master.ChangeTaskState", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
