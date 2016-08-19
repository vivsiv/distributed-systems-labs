package mapreduce

import (
	"hash/fnv"
	"os"
	"encoding/json"
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

	//Open the file and get its size
	file, err := os.Open(inFile)
	if err != nil {
		debug("DoMap error: %s", err)
		return
	}
	finfo, err := file.Stat()
	if err != nil {
		debug("DoMap error: %s", err)
		return
	}
	fsize := finfo.Size()

	//Convert the file contents to a string
	buf := make([]byte, fsize)
	_, err = file.Read(buf)
	if err != nil {
		debug("DoMap error: %s", err)
	}
	file_str := string(buf)

	kv_arr := mapF(inFile, file_str)

	//Make an array of nReduce out files to write to
	outFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		newFile, err := os.Create(reduceName(jobName, mapTaskNumber, i))
		if err != nil {
			debug("DoMap error: %s", err)
			return
		}
		outFiles[i] = newFile
	}

	//Iterate through the key value pairs, 
	//determine which out file they go to by a hash of their key
	//encode the key value pair as a json in the outFile
	for _, kv := range kv_arr {
		idx := ihash(kv.Key) % uint32(len(outFiles))
		enc := json.NewEncoder(outFiles[idx])
		err := enc.Encode(&kv)
		if (err != nil){
			debug("DoMap error: %s", err)
			return
		}
	}

	//Close out the outFiles
	for _, outFile := range outFiles {
		err := outFile.Close()
		if err != nil {
			debug("DoMap error: %s", err)
			return
		}
	}



	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
