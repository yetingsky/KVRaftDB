package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"log"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	type values []string
	m := make(map[string]values)

	// need to read one intermediate file from each map task;
	// do a loop for nMap
	for i := 0; i < nMap; i++ {
		filename := reduceName(jobName, i, reduceTaskNumber) // why I decrement 1 here?
		//fmt.Println("Reduce function open file: ", filename)
		f, err := os.OpenFile(filename, os.O_RDONLY, 0666)
		defer f.Close()
		if err != nil {
			fmt.Println("Do reduce has error: ", err)
			return
		}

		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err = decoder.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				panic(err)
			}
			// insert element to the correct map location
			m[kv.Key] = append(m[kv.Key], kv.Value)
		}	
	}

	// If the file doesn't exist, create it, or append to the file
	outputFile, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	// encode each element k,v pair from mMap to JSON, write to outout file
	enc := json.NewEncoder(outputFile)
	for k, v := range m {
		if k == "" {
			continue
		}
		err = enc.Encode(KeyValue{k, reduceF(k, v)})
		if err != nil {
			log.Fatal(err)
			break
		}
	}	

	if err := outputFile.Close(); err != nil {
		log.Fatal(err)
	}
}
