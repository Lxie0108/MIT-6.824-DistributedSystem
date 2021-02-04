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
	State      string
	MapTask    []Task
	ReduceTask []Task
}

type Task struct {
	TaskType string
	Content  string
	Filename string
	Id       int
}

// Your code here -- RPC handlers for the worker to call.

//master assigns tasks to workers
func (m *Master) ReqTask(args *ReqArgs, reply *ReqReply) error {
	switch m.State {
	case "map_state":
		for _, task := range m.MapTask {
			reply.TaskType = "map"
			reply.Filename = task.Filename
			reply.TaskId = task.Id
			reply.NReduce = m.NReduce
		}
	case "reduce_state":
		for _, task := range m.ReduceTask {
			reply.TaskType = "reduce"
			reply.Filename = task.Filename
			reply.TaskId = task.Id
			reply.NReduce = m.NReduce
		}
	}

	return nil
}

//check if all workers have finished map phase/ reduce phase, if so, move to the next phase
func(m* Master) CheckFinishPhase(args *ReqArgs, reply *ReqReply) error {{

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
	m.State = "map_state"

	//initialize map tasks, one file = one map task.
	for i, filename := range files {
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
			Id:       i,
		})
	}
	//initialize reduce tasks, there will be nReduce reduce tasks to use
	for i := 0; i < nReduce; i++ {
		m.ReduceTask = append(m.ReduceTask, Task{
			TaskType: "Reduce",
			Filename: " ",
			Content:  " ",
			Id:       i,
		})
	}

	m.server()
	return &m
}
