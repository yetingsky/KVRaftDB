package main

import (
	"strings"
	"fmt"
	"kvdb/mapreduce"
	"os"
	"strconv"
	"unicode"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func mapF(filename string, contents string) []mapreduce.KeyValue {
	
	localMap := make(map[string]int)
	// split file content to words
	f := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	words := strings.FieldsFunc(contents, f)

	for _, v := range words {
		localMap[v]++;
	}

	result := make([]mapreduce.KeyValue, len(localMap))
	for k, v := range localMap {
		var kv mapreduce.KeyValue
		kv.Value = strconv.Itoa(v)
		kv.Key = k
		result = append(result, kv)
	}
	return result;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func reduceF(key string, values []string) string {
	sum := 0
	for _, v := range values {
		if v == "" {
			continue
		}
		i, err := strconv.Atoi(v)
		if err != nil {
			fmt.Println("reduceF convert error, what is here: ", v)
			break;
		}
		sum += i
	}
	return strconv.Itoa(sum)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
