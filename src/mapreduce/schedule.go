package mapreduce

import (
	"fmt"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	doneWorkers := make(chan bool)

	for taskNum := 0; taskNum < ntasks; taskNum++ {		
		go func(mr *Master, phase jobPhase, taskNum int, nios int) {
			//Grab a free worker from the channel
			worker := <-mr.registerChannel

			//Have the free worker do a task
			args := new(DoTaskArgs)
			args.JobName = mr.jobName
			args.File = mr.files[taskNum]
			args.Phase = phase
			args.TaskNumber = taskNum
			args.NumOtherPhase = nios

			ok := call(worker, "Worker.DoTask", args, new(struct{}))
			for !ok {
				debug("Failure in %d task number %v", taskNum, phase)
				worker = <-mr.registerChannel
				ok = call(worker, "Worker.DoTask", args, new(struct{}))
				// TODO: handle worker failure
			}

			doneWorkers <-true
			//Re-register the worker as free
			mr.registerChannel <-worker

		} (mr, phase, taskNum, nios)
	}

	//Make sure all tasks have been completed
	for doneTaskNum := 0; doneTaskNum < ntasks; doneTaskNum++ {
		<-doneWorkers
	}

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
