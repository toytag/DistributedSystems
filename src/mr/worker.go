package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

func runMapf(mapf func(string, string) []KeyValue, task *Task) {
	// read from input file
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("Worker-%v: cannot open %v", os.Getpid(), task.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Worker-%v: cannot read %v", os.Getpid(), task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))

	// write to intermediate files mr-*-tmp-* in global file system
	files := make([]*os.File, task.NReduce)
	buffers := make([]*bufio.Writer, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%v-tmp-%v", os.Getpid(), i)
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Worker-%v: cannot open %v", os.Getpid(), filename)
		}
		files[i] = file
		buffers[i] = bufio.NewWriter(file)
	}

	// write to files
	for _, kv := range kva {
		hash := ihash(kv.Key) % task.NReduce
		_, err := fmt.Fprintf(buffers[hash], "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			log.Fatalf("Worker-%v: cannot write buffer %v", os.Getpid(), hash)
		}
	}

	// only flush buffer to disk when all writes are done
	for i := 0; i < task.NReduce; i++ {
		err := buffers[i].Flush()
		if err != nil {
			log.Fatalf("Worker-%v: cannot flush buffer %v to file %v", os.Getpid(), i, files[i].Name())
		}
		files[i].Close()
	}
}

func runReducef(reducef func(string, []string) string, task *Task) {
	// read from intermediate files mr-*-tmp-(Hash) in global file system
	intermediate := []KeyValue{}
	matches, err := filepath.Glob(fmt.Sprintf("mr-*-tmp-%v", task.Hash))
	if err != nil {
		log.Fatalf("Worker-%v: cannot read glob %v", os.Getpid(), task.Hash)
	}

	for _, filename := range matches {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Worker-%v: cannot open %v", os.Getpid(), filename)
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			kv := KeyValue{}
			_, err := fmt.Sscanf(line, "%v %v", &kv.Key, &kv.Value)
			if err != nil {
				log.Fatalf("Worker-%v: cannot parse %v", os.Getpid(), line)
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	// sort intermediate
	sort.Sort(ByKey(intermediate))

	// write to output file mr-out-(Hash) in global file system
	filename := fmt.Sprintf("mr-out-%v", task.Hash)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Worker-%v: cannot open %v", os.Getpid(), filename)
	}

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
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	file.Close()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	completedTask := Task{}
	for {
		assignedTask := Task{}
		ok := call("Coordinator.Exchange", &completedTask, &assignedTask)
		if ok {
			switch assignedTask.Type {
			case MAP:
				log.Printf("Worker-%v: map task assigned %v\n", os.Getpid(), assignedTask)
				runMapf(mapf, &assignedTask)
				completedTask = assignedTask
			case REDUCE:
				log.Printf("Worker-%v: reduce task assigned %v\n", os.Getpid(), assignedTask)
				runReducef(reducef, &assignedTask)
				completedTask = assignedTask
			case WAIT:
				log.Printf("Worker-%v: no task assigned, waiting...\n", os.Getpid())
				time.Sleep(time.Second)
				completedTask = assignedTask
			case EXIT:
				log.Printf("Worker-%v: all tasks done, exiting...\n", os.Getpid())
				return
			default:
				log.Fatalf("Worker-%v: unknown task type: %v", os.Getpid(), assignedTask.Type)
			}
		} else {
			fmt.Println("call failed! but not catastrophic. retrying...")
			time.Sleep(time.Second)
		}
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

	fmt.Println(err)
	return false
}
