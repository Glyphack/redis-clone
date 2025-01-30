package main

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run script.go <number_of_replicas>")
		os.Exit(1)
	}

	numReplicas, err := strconv.Atoi(os.Args[1])
	if err != nil || numReplicas < 1 {
		fmt.Println("Invalid number of replicas")
		os.Exit(1)
	}

	var wg sync.WaitGroup
	basePort := 6380

	for i := 0; i < numReplicas; i++ {
		wg.Add(1)
		port := basePort + i

		go func(port int, index int) {
			defer wg.Done()

			cmd := exec.Command("./your_program.sh", "--port", strconv.Itoa(port), "--replicaof", "localhost 6379")
			output, err := cmd.CombinedOutput()
			if err != nil {
				fmt.Printf("[Replica %d] Error: %v\n", index, err)
			}

			fmt.Printf("[Replica %d] Log:\n%s\n", index, string(output))
		}(port, i)
	}

	wg.Wait()
}
