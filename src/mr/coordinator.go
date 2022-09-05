package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "sync"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	mapCount int
	reduceCount int

	taskComplete bool
	mapFinished bool

	mapFiles []string
	mapTaskState map[int]string
	reduceTaskState map[int]string

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Coordinator) AllotTask(args *ArgsFromWorker, reply *ReplyFromCoordinator) error {

	// looks for a map task to assign
	m.mu.Lock()
	for key, val := range m.mapTaskState {
		if val == "INIT" {
			m.mapTaskState[key] = "Running"
			reply.TaskType = "Map"
			reply.TaskNum = key
			reply.FileName = m.mapFiles[key]
			reply.NumReduce = m.reduceCount

			go func(mapId int) {
				time.Sleep(10 * time.Second)

				m.mu.Lock()
				defer m.mu.Unlock()

				if m.mapTaskState[mapId] != "Complete" {
					m.mapTaskState[mapId] = "INIT"
				}
			}(key)

			m.mu.Unlock()
			return nil
		}
	}

	if !m.mapFinished {
		reply.TaskType = "Nothing"
		m.mu.Unlock()
		return nil
	}

	canFinish := true

	for key, val := range m.reduceTaskState {
		if val == "INIT" {
			m.reduceTaskState[key] = "Running"
			reply.TaskType = "Reduce"
			reply.NumReduce = m.reduceCount
			reply.TaskNum = key

			//	fmt.Println("reduce new task..")
			//	fmt.Println("key " + strconv.Itoa(key) + " val " + val)
			go func(rId int) {
				time.Sleep(10 * time.Second)

				m.mu.Lock()
				defer m.mu.Unlock()

				if m.reduceTaskState[rId] != "Complete" {
					m.reduceTaskState[rId] = "INIT"
				}
			}(key)
			m.mu.Unlock()
			return nil
		} else if val == "Running" {
			canFinish = false
		} else {
		}
	}
	m.mu.Unlock()

	if canFinish {
		reply.TaskType = "Die"
		return nil
	}
	reply.TaskType = "Nothing"
	return nil
}

func (m *Coordinator) CompletedTask(args *ArgsFromWorker, reply *ReplyFromCoordinator) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	taskType := args.TaskType
	taskNum := args.TaskNum

	//fmt.Println("from server : " + taskType + " " + strconv.Itoa(taskNum))

	if taskType == "Map"{
		m.mapTaskState[taskNum] = "Complete"
	} else if taskType == "Reduce"{
		//fmt.Println("update : " + taskType + " " + strconv.Itoa(taskNum))
		m.reduceTaskState[taskNum] = "Complete"
	}
	return nil
}

func (m *Coordinator) isMapFinished() {

	for {
		time.Sleep(100 * time.Millisecond)
		m.mu.Lock()
		done := true
		for _, val := range m.mapTaskState {
			if val != "Complete" {
				done = false
			}
		}
		m.mapFinished = done
		m.mu.Unlock()
	}
}

func (m *Coordinator) isReduceFinished() {

	for {
		time.Sleep(100 * time.Millisecond)
		m.mu.Lock()
		done := true
		for _, val := range m.reduceTaskState {
			if val != "Complete" {
				done = false
			}
		}
		m.taskComplete = done
		m.mu.Unlock()
	}
}
//
// start a thread that listens for RPCs from worker.go
//
func (m *Coordinator) server() {
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
func (m *Coordinator) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.taskComplete
}

//
// create a Coordinator.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{
		mapTaskState: make(map[int]string),
		reduceTaskState: make(map[int]string),
		mapFiles: make([]string, len(files)),
	}

	m.reduceCount = nReduce
	m.mapCount = len(files)

	for i, name := range files {
		m.mapFiles[i] = name
		m.mapTaskState[i] = "INIT"
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTaskState[i] = "INIT"
	}


	go m.isMapFinished()
	go m.isReduceFinished()

	m.server()
	return &m
}
