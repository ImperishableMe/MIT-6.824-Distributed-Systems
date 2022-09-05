package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
	//"encoding/json"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
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
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		time.Sleep(20 * time.Millisecond)
		args := ArgsFromWorker{}
		reply := ReplyFromCoordinator{}

		found := AskForTask(&args, &reply)

		if !found {
			fmt.Println("ending!")
			break
		}

		if reply.TaskType == "Map" {
			handleMap(args, reply, mapf)
		} else if reply.TaskType == "Reduce" {
			handleReduce(args, reply, reducef)
		} else if reply.TaskType == "Die" {
			break
		}
	}

}

func handleMap(args ArgsFromWorker, reply ReplyFromCoordinator,mapf func(string, string)[]KeyValue) {

	filename := reply.FileName
	nReduce := reply.NumReduce

	file, err := os.Open(filename)
	defer file.Close()
	mapId := reply.TaskNum

	files := make([]*os.File, nReduce) // temp files
	tempFileNames := make([]string, nReduce)

	for i := 0; i < nReduce; i++ {
		//name := "mr" + "-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(i)

		files[i],_ = ioutil.TempFile(".", "mapTemp")
		tempFileNames[i] = files[i].Name()
		//defer files[i].Close()
	}

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))

	//	fmt.Println("map Task: " + strconv.Itoa(mapId))
	//	fmt.Println("sz " + strconv.Itoa(len(kva)))

	for _, kv := range kva {
		rBucket := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(files[rBucket])
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println("Error while writing json!")
		}
	}


	for i := 0; i < nReduce; i++ {
		files[i].Close()
	}

	for i := 0; i < nReduce; i++ {
		name := "mr" + "-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(i)
		os.Rename(tempFileNames[i], name)
	}

	//	fmt.Printf("Completed Map %v\n", mapId)
	//
	args1 := ArgsFromWorker{TaskType: "Map", TaskNum: mapId}
	reply1 := ReplyFromCoordinator{}

	call("Coordinator.CompletedTask", &args1, &reply1)
	// tell the master you have completed it
}
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func handleReduce(args ArgsFromWorker, reply ReplyFromCoordinator, reducef func(string, []string)string){
	reduceId := reply.TaskNum
	fmt.Println("Working on Reduce No " + strconv.Itoa(reduceId))

	args1 := ArgsFromWorker{TaskType: "Reduce", TaskNum: reduceId}
	reply1 := ReplyFromCoordinator{}

	reduceFileNames ,_ := filepath.Glob("mr-*-" + strconv.Itoa(reduceId))

	intermediate := []KeyValue{}
	for _, filename := range reduceFileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reduceId)
	ofile, _ := ioutil.TempFile(".", "reduce")
	tmpName := ofile.Name()

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

	os.Rename(tmpName, oname)

	call("Coordinator.CompletedTask", &args1, &reply1)
}


func AskForTask(args *ArgsFromWorker, reply *ReplyFromCoordinator) bool {

	return call("Coordinator.AllotTask", args, reply)
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
