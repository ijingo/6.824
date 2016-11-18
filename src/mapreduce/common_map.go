package mapreduce

import (
	"hash/fnv"
	"encoding/json"
	"io/ioutil"
	"os"
	"log"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

	buff, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("Mapper read input error ", err)
	}

	res := mapF(inFile, string(buff))

	for i := 0; i < nReduce; i++ {
		outFileName := reduceName(jobName, mapTaskNumber, i)
		outFile, err := os.Create(outFileName)
		defer outFile.Close()
		if err != nil {
			log.Fatalf("Mapper create file %v error %v\n", outFileName, err)
		}
		enc := json.NewEncoder(outFile)
		for _, kv := range res {
			if ihash(kv.Key)%uint32(nReduce) == uint32(i) {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("Encode %v error %v\n", kv, err)
				}
			}
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
