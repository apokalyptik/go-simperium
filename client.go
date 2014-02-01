package simperium

// BUG(apokalyptik) An auth failure on the *first* Bucket disconnects the Client socket... which has been a pain to get working so far
// granted not much effort has been spent on the issue yet... Requires further work. Auth failures on subsequent buckets after a successful
// connection do not cause this problem (which is rooted in simperium hanging up on us)

import (
	"code.google.com/p/go-uuid/uuid"
	"code.google.com/p/go.net/websocket"
	"fmt"
	"io"
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
	recvChans []chan string
	recvCount []func()
	channels  int

	debug bool

	lock sync.Mutex
}

func (c *Client) Debug(debug bool) {
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
	c.recvChans = append(c.recvChans, bucket.recv)
	c.recvCount = append(c.recvCount, bucket.newMessage)
	c.channels++

	go func(channel int, bucket, client chan string) {
		var prepend = fmt.Sprintf("%d:", channel)
		for {
			m := <-bucket
			client <- prepend + m
		}
	}(channel, bucket.send, c.socketSend)
	if err := bucket.auth(); err != nil {
		//TODO: cleanup
		//channel--
		//c.recvChans = c.recvChans[:len(c.recvChans)-1]
		//c.recvCount = c.recvCount[:len(c.recvCount)-1]
		delete(c.buckets, key)
		bucket.drain()
		return nil, err
	}
	return bucket, nil
}

func (c *Client) mindHeartbeats() {
	tick := time.Tick(time.Duration(15 * time.Second))
	count := 0
	for {
		<-tick
		if c.socket == nil {
			continue
		}
		c.socketSend<- fmt.Sprintf("h:%d", count)
		count += 2
	}
}

func (c *Client) mindSocketWrites() {
	for {
		b := <-c.socketSend
		err := websocket.Message.Send(c.socket, b)
		if err != nil {
			c.log("simperium.Client.mindSocketWrites websocket.Message.Send error: %s", err.Error())
			c.socketError <- err
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
			if err != io.EOF {
				c.log("simperium.Client.mindSocketReads websocket.Message.Receive error: %s", err.Error())
				c.socketError <- err
				return
			}
		}
		c.log("<<< %s", message)
		c.socketRecv <- message
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
	c.socketRecv = make(chan string)
	c.socketSend = make(chan string)
	c.recvChans = make([]chan string, 0)
	c.socketError = make(chan error)
	socket, err := websocket.Dial(
		"wss://api.simperium.com:443/sock/websocket",
		"",
		"https://github.com/apokalyptik/go-simperium")
	if err != nil {
		return err
	}
	c.socket = socket
	go c.mindSocketReads()
	go c.handleSocketReads()
	go c.mindSocketWrites()
	go c.mindHeartbeats()
	return nil
}
