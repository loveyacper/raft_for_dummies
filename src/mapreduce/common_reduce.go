package mapreduce

import (
    "fmt"
    "sort"
    "encoding/json"
    "os"
)
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
    for i := 0; i < nMap; i++ {
        var myfile string = reduceName(jobName, i, reduceTask)
        var kvs = make(map[string] []string) // unordered_map
        var keys []string // for sort keys

        fmt.Println("read myfile " + myfile)
        f, err := os.OpenFile(myfile, os.O_RDONLY, 0)
        if err != nil {
            fmt.Println("open err " + err.Error())
        } else {
            dec := json.NewDecoder(f)
            for {
                var kv KeyValue
                err = dec.Decode(&kv)
                if err != nil {
                    break
                }

                if _, ok := kvs[kv.Key]; !ok {
                    keys = append(keys, kv.Key) // new key
                }

                kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
            }
        }

        sort.Strings(keys)
        out, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_RDWR,0666)
        if err != nil {
            fmt.Println("Create file %s failed", outFile)
            return
        }

        enc := json.NewEncoder(out)
        for _, key := range keys {
            v := reduceF(key, kvs[key])
            if err = enc.Encode(KeyValue{key, v}); err != nil {
                fmt.Println("write [key: %s] to file %s failed", key, outFile)
            }
        }
        out.Close()
    }
    fmt.Println("reduce Out file " + outFile)
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
	// Your code here (Part I).
	//
}
