#!/bin/bash

GOPATH=/home/mushroom/go/src/6.824
export GOPATH

run() {
    number=$1
    shift
    for i in `seq $number`; do
      $@
    done
}

#run 5 go test -run 2A
#run 50 go test -run TestRejoin2B
#run 20 go test -run TestXiaying
#run 50 go test -run TestBasicAgree2B


go test -run 2A
#go test -run 2B

#go test -run 2C
#go test -run TestReliableChurn2C




#run 10 go test -run 2A
#run 10 go test -run 2B

#run 10 go test -run TestPersist12C
#run 10 go test -run TestPersist22C
#run 10 go test -run TestPersist32C
#run 10 go test -run TestUnreliableAgree2C
#run 10 go test -run TestReliableChurn2C

# ./run.sh > result.txt
# grep "Passed" result.txt | wc -l
