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


	for taskNum := 0; taskNum < ntasks; taskNum++ {
		go func(mr *Master, phase jobPhase, ntasks int, nios int) {
			worker := <-mr.registerChannel

			args := new(DoTaskArgs)
			args.JobName = mr.jobName
			args.File = mr.files[taskNum]
			args.Phase = phase
			args.TaskNumber = taskNum
			args.NumOtherPhase = nios

			ok := call(worker, "Worker.DoTask", args, new(struct{}))
			if !ok {
				// handle worker failure
				debug("Failure in %d task number %v", taskNum, phase)
			}
			mr.doneChannel <- true
			mr.registerChannel <- worker

		} (mr, phase, taskNum, nios)
	}

	for doneTaskNum := 0; doneTaskNum < ntasks; doneTaskNum++ {
		<-mr.doneChannel
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
