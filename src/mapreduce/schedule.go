package mapreduce

import (
	"fmt"
	"sync"
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

	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(taskNumber int, nios int, phase jobPhase) {
			defer wg.Done()
			for {
				worker := <-mr.registerChannel
				args := DoTaskArgs{mr.jobName, mr.files[taskNumber], phase, taskNumber, nios}
				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				if ok {
					go func() {
						mr.registerChannel <- worker
					}()
					break
				}
			}
		}(i, nios, phase)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
