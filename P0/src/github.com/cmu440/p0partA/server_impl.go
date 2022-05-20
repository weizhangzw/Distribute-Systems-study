// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/cmu440/p0partA/kvstore"
)

const MAX_MESSAGE_QUEUE_LENGTH = 500

type client struct {
	connection   net.Conn
	messageQueue chan []byte
	// Quit signals for goroutine.
	quitSignal_Read  chan int
	quitSignal_Write chan int
}

// Database command
type db struct {
	reqType requestType
	key     string
	value   []byte
	client  *client
}

type requestType int

const (
	Get    requestType = iota + 1 // 1
	Put                           // 2
	Delete                        // 3
)

// Implementation of keyValueServer
type keyValueServer struct {
	store          kvstore.KVStore
	listener       net.Listener
	currentClients []*client
	deadClients    []*client
	// Server commands
	newConnection chan net.Conn
	deadClient    chan *client
	dbQuery       chan *db
	countClients  chan int
	clientCount   chan int
	countDropped  chan int
	dropped       chan int
	// Quit signals for goroutine.
	quitSignal_Main   chan int
	quitSignal_Accept chan int
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	return &keyValueServer{
		store,
		nil,
		nil,
		nil,
		make(chan net.Conn),
		make(chan *client),
		make(chan *db),
		make(chan int),
		make(chan int),
		make(chan int),
		make(chan int),
		make(chan int),
		make(chan int),
	}
}

func (kvs *keyValueServer) Start(port int) error {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		fmt.Printf("Failed to listen port %d. Error: %s\n", port, err)
		return err
	}

	kvs.listener = ln

	go mainRoutine(kvs)
	go acceptRoutine(kvs)

	return nil
}

func (kvs *keyValueServer) Close() {
	kvs.listener.Close()
	// Turn off mainRoutine and acceptRoutine.
	kvs.quitSignal_Main <- 0
	kvs.quitSignal_Accept <- 0
}

func (kvs *keyValueServer) CountActive() int {
	kvs.countClients <- 0
	return <-kvs.clientCount
}

func (kvs *keyValueServer) CountDropped() int {
	kvs.countDropped <- 0
	return <-kvs.dropped
}

func acceptRoutine(kvs *keyValueServer) {
	defer fmt.Println("\"acceptRoutine\" ended")

	for {
		select {
		case <-kvs.quitSignal_Accept:
			return
		default:
			// This is blocking, and will return when a connection is established or
			// the listener is closed.
			conn, err := kvs.listener.Accept()
			if err == nil {
				kvs.newConnection <- conn
			}
		}
	}
}

func mainRoutine(kvs *keyValueServer) {
	defer fmt.Println("\"mainRoutine\" ended")

	for {
		select {
		// Add a new client to the client list.
		case newConnection := <-kvs.newConnection:
			c := &client{
				newConnection,
				make(chan []byte, MAX_MESSAGE_QUEUE_LENGTH),
				make(chan int),
				make(chan int)}
			kvs.currentClients = append(kvs.currentClients, c)
			go readRoutine(kvs, c)
			go writeRoutine(c)

		// Remove dead client.
		case deadClient := <-kvs.deadClient:
			for i, c := range kvs.currentClients {
				if c == deadClient {
					kvs.currentClients =
						append(kvs.currentClients[:i], kvs.currentClients[i+1:]...)
					kvs.deadClients = append(kvs.deadClients, c)
					break
				}
			}

		// Execute DB query.
		case request := <-kvs.dbQuery:
			if request.reqType == Get {
				s := kvs.store.Get(request.key)
				for _, v := range s {
					message := append(append([]byte(request.key), ":"...), v...)
					if len(request.client.messageQueue) < MAX_MESSAGE_QUEUE_LENGTH {
						request.client.messageQueue <- message
					}
				}
			} else if request.reqType == Put {
				kvs.store.Put(request.key, request.value)
			} else { // request.reqType == Delete
				kvs.store.Delete(request.key)
			}

		// Get the number of alive clients.
		case <-kvs.countClients:
			kvs.clientCount <- len(kvs.currentClients)

		// Get the number of dropped clients
		case <-kvs.countDropped:
			kvs.dropped <- len(kvs.deadClients)

			// Quit main routine
		case <-kvs.quitSignal_Main:
			for _, c := range kvs.currentClients {
				c.connection.Close()
				c.quitSignal_Write <- 0
				c.quitSignal_Read <- 0
			}
			return
		}
	}
}

func readRoutine(kvs *keyValueServer, c *client) {
	clientReader := bufio.NewReader(c.connection)

	// Read in from connection
	for {
		select {
		case <-c.quitSignal_Read:
			return
		default:
			message, err := clientReader.ReadBytes('\n')
			if err == io.EOF {
				kvs.deadClient <- c
			} else if err != nil {
				return
			} else {
				tokens := bytes.Split(message, []byte(":"))
				reqTypeToken := string(tokens[0])
				if reqTypeToken == "Put" {
					key := string(tokens[1][:])
					value := strip(tokens[2])
					kvs.dbQuery <- &db{
						reqType: Put,
						key:     key,
						value:   value,
						client:  c,
					}
				} else if reqTypeToken == "Get" {
					key := string(strip(tokens[1]))
					kvs.dbQuery <- &db{
						reqType: Get,
						key:     key,
						client:  c,
					}
				} else { // "Delete"
					key := string(strip(tokens[1]))
					kvs.dbQuery <- &db{
						reqType: Delete,
						key:     key,
						client:  c,
					}
				}
			}
		}
	}
}

func writeRoutine(c *client) {
	for {
		select {
		case <-c.quitSignal_Write:
			return
		case message := <-c.messageQueue:
			message = append(message, "\n"...)
			c.connection.Write(message)
		}
	}
}

func strip(b []byte) []byte {
	return b[:len(b)-1]
}
