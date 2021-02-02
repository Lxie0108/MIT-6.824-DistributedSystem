package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	// Your definitions here.
	NReduce    int
	MapDone    bool
	ReduceDone bool
	MapTask    []Task
	ReduceTask []Task
}

type Task struct {
	TaskType string
	Content  string
	Filename string
}

// Your code here -- RPC handlers for the worker to call.

//master assigns tasks to workers
func (m *Master) AssignTask(args *ReqArgs, reply *ReqReply) error {
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = m.ReduceDone
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.MapTask = nil
	m.ReduceTask = nil
	m.NReduce = nReduce
	m.MapDone = false
	m.ReduceDone = false

	//initialize map tasks, one file = one map task.
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}

		file.Close()

		m.MapTask = append(m.MapTask, Task{
			TaskType: "Map",
			Filename: filename,
			Content:  string(content),
		})
	}

	for i := 0; i < nReduce; i++ {
		m.ReduceTask = append(m.ReduceTask, Task{
			TaskType: "Reduce",
		})
	}

	m.server()
	return &m
}
