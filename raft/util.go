package raft

import (
	"log"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const RPCMaxTries = 2

// SendRPCRequest will attempt a request `RPCMaxTries` tries
func SendRPCRequest(request func() bool) bool {
	makeRequest := func(successChan chan int) {
		if ok := request(); ok {
			successChan <- 1
		} else {
			successChan <- 0
		}
	}

	for attempts := 0; attempts < RPCMaxTries; attempts++ {
		rpcChan := make(chan int, 1)
		go makeRequest(rpcChan)
		
		result := <-rpcChan
		if result == 1 {
			return true
		}
	}
	return false
}
