package simperium

// BUG(apokalyptik) closing the connection, and then reopening it does not "reconnect" connected buckets. 
// Further it does not cause a reindexing of said buckets

import (
	"code.google.com/p/go-uuid/uuid"
	"code.google.com/p/go.net/websocket"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var channelMessage *regexp.Regexp = regexp.MustCompile("^\\d+:")

// A client connection to simperium. One persistent websocket connection is maintained via this
// parent structure. The child structures (Buckets) communicate through this to talk to simperium
// via websocket channels. Each channel (and thus bucket) is authenticated individually which is
// why no app/bucket/token is needed when creating this structure.
type Client struct {
	clientId string
	socket   *websocket.Conn

	socketError chan error
	socketRecv  chan string
	socketSend  chan string

	buckets   map[string]*Bucket
	bucketId  []*Bucket
	recvChans map[int]chan string
	recvCount map[int]func()
	channels  int
	liveChannels int

	connectedClient chan bool
	initialized bool

	heartbeat time.Duration
	debug bool

	socketLock sync.Mutex
	lock sync.Mutex
}

func (c *Client) SetHeartbeat(dur time.Duration) {
	c.heartbeat = dur
}

func (c *Client) SetDebug(debug bool) {
	c.debug = debug
}

func (c *Client) log(data ...interface{}) {
	if c.debug {
		switch len(data) {
		case 0:
			return
		case 1:
			log.Printf(data[0].(string))
		default:
			log.Printf(data[0].(string), data[1:]...)
		}
	}
}

// Instantiate a connection to a Simperium Bucket communicating through a new channel
// on this clients existing websocket connection
func (c *Client) Bucket(app, name, token string) (*Bucket, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.socket == nil {
		c.Connect()
	}
	key := fmt.Sprintf("%s:%s:%s", name, token, token)
	if bucket, ok := c.buckets[key]; ok {
		return bucket, nil
	}
	bucket := new(Bucket)
	bucket.init()
	bucket.app = app
	bucket.name = name
	bucket.token = token
	bucket.send = make(chan string)
	bucket.recv = make(chan string)
	bucket.clientId = c.clientId

	channel := c.channels
	c.recvChans[channel] = bucket.recv
	c.recvCount[channel] = bucket.newMessage
	c.channels++
	c.liveChannels++

	go func(channel int, bucket, client chan string) {
		var prepend = fmt.Sprintf("%d:", channel)
		for {
			m := <-bucket
			client <- prepend + m
		}
	}(channel, bucket.send, c.socketSend)
	if err := bucket.auth(); err != nil {
		c.liveChannels--
		if c.liveChannels == 0 {
			c.closeSocket()
		}
		delete(c.buckets, key)
		bucket.drain()
		return nil, err
	}
	return bucket, nil
}

func (c *Client) mindHeartbeats() {
	count := 0
	for {
		<-time.After(c.heartbeat)
		if c.socket == nil {
			continue
		}
		c.socketSend<- fmt.Sprintf("h:%d", count)
		count += 2
	}
}

func (c *Client) closeSocket() {
	c.socketLock.Lock()
	defer c.socketLock.Unlock()
	if c.socket != nil {
		c.socket.Close()
		c.socket = nil
	}
}

func (c *Client) mindSocketWrites() {
	for {
		b := <-c.socketSend
		err := websocket.Message.Send(c.socket, b)
		if err != nil {
			c.log("simperium.Client.mindSocketWrites websocket.Message.Send error: %s", err.Error())
			c.closeSocket()
			return
		}
		c.log(">>> %s", b)
	}
}

func (c *Client) mindSocketReads() {
	var message string
	for {
		err := websocket.Message.Receive(c.socket, &message)
		if err != nil {
			c.log("simperium.Client.mindSocketWrites websocket.Message.Send error: %s", err.Error())
			c.closeSocket()
			return
		}
		c.log("<<< %s", message)
		c.socketRecv <- message
	}
}

func (c *Client) mindSocketReadWrite() {
	for {
		go c.mindSocketWrites()
		go c.mindSocketReads()
		<-c.connectedClient
	}
}

func (c *Client) sendChannel(channel int, m string) {
	c.recvChans[channel] <- m
}

func (c *Client) handleSocketReads() {
	var hb string = "h:"
	var chanCache = make(map[string]int)
	for {
		m := <-c.socketRecv
		switch {
		case strings.HasPrefix(m, hb):
			break
		case channelMessage.MatchString(m):
			parts := strings.SplitN(m, ":", 2)
			if channel, ok := chanCache[string(parts[0])]; ok {
				c.recvCount[channel]()
				go c.sendChannel(channel, parts[1])
			} else {
				channel, _ := strconv.Atoi(string(parts[0]))
				chanCache[string(parts[0])] = channel
				c.recvCount[channel]()
				go c.sendChannel(channel, parts[1])
			}
			break
		default:
			c.log("unknown socket read: %s", string(m))
		}
	}
}

func (c *Client) Connect() error {
	c.clientId = uuid.NewUUID().String()
	if false == c.initialized {
		c.socketRecv = make(chan string)
		c.socketSend = make(chan string)
		c.recvChans = make(map[int]chan string)
		c.recvCount = make(map[int]func())
		c.socketError = make(chan error)
		c.connectedClient = make(chan bool)
	}
	socket, err := websocket.Dial(
		"wss://api.simperium.com:443/sock/websocket",
		"",
		"https://github.com/apokalyptik/go-simperium")
	if err != nil {
		return err
	}
	c.socket = socket
	if false == c.initialized {
		go c.mindSocketReadWrite()
		go c.handleSocketReads()
		go c.mindHeartbeats()
		c.initialized = true
	} else {
		c.connectedClient<- true // Wake reader and writer again
	}
	return nil
}
