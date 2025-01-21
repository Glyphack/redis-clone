package main

import (
	"fmt"
	"net"
	"sync"
	"time"
)

func pingClient(id int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		fmt.Printf("Client %d: Failed to connect: %v\n", id, err)
		return
	}
	defer conn.Close()

	pingCmd := []byte("*1\r\n$4\r\nping\r\n")
	buffer := make([]byte, 1024)

	for {
		_, err = conn.Write(pingCmd)
		fmt.Printf("Client %d: Sent PING\n", id)
		if err != nil {
			fmt.Printf("Client %d: Failed to send PING: %v\n", id, err)
			return
		}

		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Client %d: Failed to read response: %v\n", id, err)
			return
		}

		fmt.Printf("Client %d: Received: %s", id, buffer[:n])
		time.Sleep(100 * time.Millisecond) // Small delay between pings
	}
}

func main() {
	var wg sync.WaitGroup
	numClients := 3

	fmt.Printf("Starting %d concurrent clients...\n", numClients)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go pingClient(i+1, &wg)
	}

	wg.Wait()
}
