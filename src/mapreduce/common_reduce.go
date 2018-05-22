package mapreduce

import (
	"encoding/json"
	"fmt"
	"sort"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.

	// Opening all temp files
	var decoders = make([]*json.Decoder, nMap)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		fd, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
		if err != nil {
			logger.Println(fmt.Sprintf("Failed to open: %s", fileName))
			return
		}
		decoders[i] = json.NewDecoder(fd)
		defer fd.Close()
	}

	kvs := make(map[string][]string)

	// Unmarshal all temp files and collect key-values
	for i := 0; i < nMap; i++ {
		var kv *KeyValue
		for {
			err := decoders[i].Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}
	//logger.Println(fmt.Sprintf("Read %d keys from %d files", len(kvs), nMap))

	// Sort by key
	logger.Println(jobName, reducePhase, reduceTask, fmt.Sprintf("Sorting data by keys"))
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create output file
	fd, err := os.OpenFile(outFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	defer fd.Close()
	if err != nil {
		logger.Println(jobName, reducePhase, reduceTask, fmt.Sprintf("Failed to open: %s", outFile))
		return
	}

	// Apply reduce f() and write results
	logger.Println(jobName, reducePhase, reduceTask, fmt.Sprintf("Applying reduce f() and writing to output file: %s", outFile))
	encoder := json.NewEncoder(fd)
	for _, key := range keys {
		// apply the reduce function on every k-v pair
		encoder.Encode(KeyValue{key, reduceF(key, kvs[key])})
	}
}
