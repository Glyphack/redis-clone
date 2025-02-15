package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	numClients    = 10
	opsPerClient  = 1000
	keyRange      = 100
	valueRange    = 1000
	serverAddress = "localhost:6379"
)

type Command struct {
	cmd      string
	expected string
}

var (
	state     = make(map[string]string)
	stateLock sync.RWMutex
)

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func formatCommand(args ...string) string {
	cmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		cmd += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	return cmd
}

func parseResponse(resp string) string {
	resp = strings.TrimSpace(resp)
	if len(resp) == 0 {
		return ""
	}

	switch resp[0] {
	case '+':
		return resp[1:]
	case '$':
		parts := strings.SplitN(resp, "\r\n", 3)
		if len(parts) < 3 {
			return ""
		}
		return parts[1]
	case '-':
		return "ERROR: " + resp[1:]
	default:
		return resp
	}
}

type Client struct {
	id         int
	conn       net.Conn
	reader     *bufio.Reader
	successOps int
	failedOps  int
}

func (c *Client) executeCommand(cmd Command) error {
	_, err := c.conn.Write([]byte(cmd.cmd))
	if err != nil {
		return fmt.Errorf("write error: %v", err)
	}

	response, err := c.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read error: %v", err)
	}

	if strings.HasPrefix(response, "$") {
		length := strings.TrimSpace(response[1:])
		if length != "-1" {
			response, err = c.reader.ReadString('\n')
			if err != nil {
				return fmt.Errorf("read error: %v", err)
			}
		}
	}

	parsed := parseResponse(response)
	if parsed != cmd.expected && cmd.expected != "*" {
		return fmt.Errorf("response mismatch: expected '%s', got '%s'", cmd.expected, parsed)
	}

	return nil
}

func (c *Client) runOperations(wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.conn.Close()

	for i := 0; i < opsPerClient; i++ {
		if rand.Float32() < 0.7 {
			key := fmt.Sprintf("key:%d:%d", c.id, rand.Intn(keyRange))
			value := randomString(rand.Intn(20) + 1)

			stateLock.Lock()
			state[key] = value
			stateLock.Unlock()

			cmd := Command{
				cmd:      formatCommand("SET", key, value),
				expected: "OK",
			}

			if err := c.executeCommand(cmd); err != nil {
				fmt.Printf("Client %d SET error: %v\n", c.id, err)
				c.failedOps++
				continue
			}

			verifyCmd := Command{
				cmd:      formatCommand("GET", key),
				expected: value,
			}

			if err := c.executeCommand(verifyCmd); err != nil {
				fmt.Printf("Client %d verification error: %v\n", c.id, err)
				c.failedOps++
				continue
			}

		} else {
			stateLock.RLock()
			keys := make([]string, 0, len(state))
			for k := range state {
				if strings.HasPrefix(k, fmt.Sprintf("key:%d:", c.id)) {
					keys = append(keys, k)
				}
			}
			stateLock.RUnlock()

			if len(keys) == 0 {
				continue
			}

			key := keys[rand.Intn(len(keys))]
			stateLock.RLock()
			expectedValue := state[key]
			stateLock.RUnlock()

			cmd := Command{
				cmd:      formatCommand("GET", key),
				expected: expectedValue,
			}

			if err := c.executeCommand(cmd); err != nil {
				fmt.Printf("Client %d GET error: %v\n", c.id, err)
				c.failedOps++
				continue
			}
		}
		c.successOps++
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	var wg sync.WaitGroup
	clients := make([]*Client, numClients)
	totalOps := 0
	totalFailures := 0

	fmt.Printf("Starting consistency test with %d clients, %d operations each...\n", numClients, opsPerClient)

	for i := 0; i < numClients; i++ {
		conn, err := net.Dial("tcp", serverAddress)
		if err != nil {
			fmt.Printf("Failed to connect client %d: %v\n", i, err)
			continue
		}

		clients[i] = &Client{
			id:     i,
			conn:   conn,
			reader: bufio.NewReader(conn),
		}

		wg.Add(1)
		go clients[i].runOperations(&wg)
	}

	wg.Wait()

	for i, client := range clients {
	for i, client := range clients {
		if client == nil {
			continue
		}
		fmt.Printf("Client %d: %d successful operations, %d failed operations\n",
			i, client.successOps, client.failedOps)
		totalOps += client.successOps
		totalFailures += client.failedOps
	}

	fmt.Printf("\nTest completed:\n")
	fmt.Printf("Total successful operations: %d\n", totalOps)
	fmt.Printf("Total failed operations: %d\n", totalFailures)
	fmt.Printf("Success rate: %.2f%%\n", float64(totalOps)*100/float64(totalOps+totalFailures))
	fmt.Printf("Final state size: %d keys\n", len(state))
}
