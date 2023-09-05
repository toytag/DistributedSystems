package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// RPC definitions

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	WAIT
	EXIT
)

type Task struct {
	Type     TaskType
	Filename string
	NReduce  int
	Hash     int
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
