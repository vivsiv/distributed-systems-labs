package mapreduce

import (
	"os"
	"encoding/json"
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
	//Map each key to a slice of its values
	keysMap := make(map[string][]string)
	//array of keys to be sorted later
	keysSorted := make([]string, 0)

	//For each mapTask grab the appropriate reduce file
	for i := 0; i < nMap; i++ {
		mapFileName := reduceName(jobName, i, reduceTaskNumber)
		mapFile, err := os.Open(mapFileName)
		if err != nil {
			debug("DoReduce error: %s", err)
		}
		dec := json.NewDecoder(mapFile)		
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break;
			}
			_, ok := keysMap[kv.Key]
			if !ok {
				keysMap[kv.Key] = make([]string, 0)		
				keysSorted = append(keysSorted, kv.Key)	
			}
			keysMap[kv.Key] = append(keysMap[kv.Key], kv.Value)
		}
		mapFile.Close()
	}
	sort.Strings(keysSorted)

	mergeFile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	if err != nil {
			debug("DoReduce error: %s", err)
		}
	enc := json.NewEncoder(mergeFile)
	//Run the reduce function for each key and slice of values
	for _, key := range keysSorted {
		reduceOut := reduceF(key, keysMap[key])
		kvReduced := KeyValue{Key:key, Value:reduceOut}
		enc.Encode(&kvReduced)
	}

	err = mergeFile.Close()
	if err != nil {
		debug("DoMap error: %s", err)
	}
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
}
