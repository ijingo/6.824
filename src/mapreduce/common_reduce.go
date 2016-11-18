package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvs := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		inFileName := reduceName(jobName, i, reduceTaskNumber)
		inFile, err := os.Open(inFileName)
		defer inFile.Close()
		if err != nil {
			log.Fatalf("Open %v fail %v\n", inFileName, err)
		}

		dec := json.NewDecoder(inFile)

		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			_, init := kvs[kv.Key]
			if !init {
				kvs[kv.Key] = make([]string, 0)
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}

	var keys []string
	for k, _ := range kvs {
		keys = append(keys, k)
	}
	
	sort.Strings(keys)

	outFileName := mergeName(jobName, reduceTaskNumber)

	outFile, err := os.Create(outFileName)
	defer outFile.Close()
	if err != nil {
		log.Fatalf("Create %v fail %v\n", outFileName, err)
	}

	enc := json.NewEncoder(outFile)

	for _, k := range keys {
		res := reduceF(k, kvs[k])
		enc.Encode(&KeyValue{k, res})
	}
}
