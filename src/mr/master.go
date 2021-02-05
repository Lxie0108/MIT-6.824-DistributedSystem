package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	NReduce    int
	NMap       int
	State      string
	MapTask    []Task
	ReduceTask []Task
	mu         sync.Mutex
}

type Task struct {
	TaskType string
	Content  string
	Filename string
	State    string
	Id       int
	Time     time.Time
}

// Your code here -- RPC handlers for the worker to call.

//master assigns tasks to workers
func (m *Master) ReqTask(args *ReqArgs, reply *ReqReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.State {
	case "map_state":
		for i, task := range m.MapTask {
			if task.State == "idle" || (task.State == "busy" && time.Now().Sub(task.Time) >= 10*time.Second) { //second condition, give up stalled workers
				reply.TaskType = "map"
				reply.Filename = task.Filename
				reply.TaskId = task.Id
				reply.NReduce = m.NReduce
				m.MapTask[i].State = "busy"
				m.MapTask[i].Time = time.Now()
				return nil
			}
		}
	case "reduce_state":
		for i, task := range m.ReduceTask {
			if task.State == "idle" || (task.State == "busy" && time.Now().Sub(task.Time) >= 10*time.Second) {
				reply.TaskType = "reduce"
				reply.Filename = task.Filename
				reply.TaskId = task.Id
				reply.NReduce = m.NReduce
				reply.NMap = m.NMap
				m.ReduceTask[i].State = "busy"
				m.ReduceTask[i].Time = time.Now()
				return nil
			}
		}
	}
	return nil
}

//change task state to "complete"
func (m *Master) ChangeTaskState(args *ChangeTaskStateArgs, reply *ChangeTaskStateReply) error {
	if args.TaskType == "map" {
		m.MapTask[args.TaskId].State = "map_complete"
		//fmt.Printf("task state %s", m.MapTask[args.TaskId].State)
	} else if args.TaskType == "reduce" {
		m.ReduceTask[args.TaskId].State = "reduce_complete"
	}
	m.CheckFinishPhase()
	return nil
}

//check if all workers have finished map phase/ reduce phase, if so, move to the next phase
func (m *Master) CheckFinishPhase() error {
	//fmt.Printf("reach checkFinish")
	switch m.State {
	case "map_state":
		for _, task := range m.MapTask {
			if task.State != "map_complete" {
				return nil
			}
		}
		m.State = "reduce_state"
	case "reduce_state":
		for _, task := range m.ReduceTask {
			if task.State != "reduce_complete" {
				return nil
			}
		}
		m.State = "complete"
	}
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
	if m.State == "complete" {
		ret = true
	}
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
	m.MapTask = []Task{}
	m.ReduceTask = []Task{}
	m.NReduce = nReduce
	m.NMap = len(files)
	m.State = " "
	m.mu = sync.Mutex{}

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
		defer file.Close()

		m.MapTask = append(m.MapTask, Task{
			TaskType: "map",
			Filename: filename,
			Content:  string(content),
			State:    "idle",
			Id:       i,
		})
		//file.Close()
	}
	//initialize reduce tasks, there will be nReduce reduce tasks to use
	for i := 0; i < nReduce; i++ {
		m.ReduceTask = append(m.ReduceTask, Task{
			TaskType: "reduce",
			Filename: " ",
			Content:  " ",
			State:    "idle",
			Id:       i,
		})
	}
	m.State = "map_state"
	m.server()
	return &m
}
