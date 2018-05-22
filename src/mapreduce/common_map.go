package mapreduce

import (
	"hash/fnv"
	"encoding/json"
	"os"
	"fmt"
	"io/ioutil"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, 	// which map task this is
	inFile string,
	nReduce int, 	// the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	//
	// doMap manages one map task: it should read one of the input files
	// (inFile), call the user-defined map function (mapF) for that file's
	// contents, and partition mapF's output into nReduce intermediate files.
	//

	data, err := ioutil.ReadFile(inFile)
	if err != nil {
		logger.Println(err.Error())
		return // not continue
	}

	// Apply map f()
	kvs := mapF(inFile, string(data))

	// Opening all temp files
	var encoders = make([]*json.Encoder, nReduce)
	var fd *os.File = nil
	defer fd.Close()
	for i := 0; i < nReduce; i++ {
		// create a new file for each reduce task
		fileName := reduceName(jobName, mapTask, i)
		fd, err = os.OpenFile(fileName, os.O_CREATE | os.O_APPEND | os.O_WRONLY, 0600)
		if err != nil {
			logger.Println(fmt.Sprintf("Failed to open: %s", fileName))
			return
		}
		encoders[i] = json.NewEncoder(fd)
	}

	// Marshal and write all K-V pairs to temp files
	for _, kv := range kvs {
		r := ihash(kv.Key) % nReduce // calculate which destination file should the results be written to
		err = encoders[r].Encode(kv)
		if err != nil {
			logger.Println(fmt.Sprintf("Failed to marshal/write k: %s, v: %s", kv.Key, kv.Value))
			return
		}
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
