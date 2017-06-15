package raft

import "log"
import "os"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
	    f, err := os.OpenFile("log_raft", os.O_APPEND | os.O_CREATE | os.O_RDWR, 0666)
		if err != nil {
			log.Printf("error opening file: %v", err)
	    }
		defer f.Close()
		log.SetOutput(f)
		log.Printf(format, a...)
	}
	return
}
