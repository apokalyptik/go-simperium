package simperium

import (
	"code.google.com/p/go-uuid/uuid"
	"code.google.com/p/go.net/websocket"
	"crypto/tls"
	"fmt"
	"log"
	"net/url"
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

	wssUrl string

	socketError chan error
	socketRecv  chan string
	socketSend  chan string

	buckets      map[string]*Bucket
	bucketId     []*Bucket
	recvChans    map[int]chan string
	recvCount    map[int]func()
	channels     int
	liveChannels int

	writeQueue  []string
	connectedAt time.Time
	lastReadAt  time.Time
	readTimeout time.Duration

	connectedClient chan bool
	initialized     bool

	heartbeat time.Duration
	debug     bool

	socketLock sync.Mutex
	lock       sync.Mutex
}

func (c *Client) SetHeartbeat(dur time.Duration) {
	c.heartbeat = dur
	c.readTimeout = dur * 3
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
	} else {
		c.buckets[key] = bucket
	}
	return bucket, nil
}

func (c *Client) mindDisconnects() {
	for {
		if c.lastReadAt.After(c.connectedAt) {
			if time.Since(c.lastReadAt) > c.readTimeout {
				c.closeSocket()
				if err := c.Connect(); err != nil {
					<-time.After(time.Duration(50 * time.Millisecond))
				}
				continue
			}
		}
		<-time.After(c.heartbeat)
	}
}

func (c *Client) mindHeartbeats() {
	count := 0
	for {
		<-time.After(c.heartbeat)
		if c.socket == nil {
			continue
		}
		c.socketSend <- fmt.Sprintf("h:%d", count)
		count += 2
	}
}

func (c *Client) closeSocket() {
	if c.socket == nil {
		return
	}
	c.socketLock.Lock()
	defer c.socketLock.Unlock()
	if c.socket != nil {
		c.socket.Close()
		c.socket = nil
	}
}

func (c *Client) mindSocketWrites() {
	var message string

	// The write can panic
	defer func() {
		// BUG(apokalyptik) Client -- We may lose a write in this recover. Need to see if we can get a message into c.writeQueue
		// from here
		recover()
	}()

	if len(c.writeQueue) > 0 {
		// Upon starting... Check whether we have pending writes in our emergency queue
		// If so make that queue local, and replace it with an empty queue, and then spawn
		// a goroutine to feed the now local queue entries back into the send channel.
		// No lock is necessary here because the only accesses to this queue come later in
		// this same function and there is only one instance of this function running for
		// this client globally
		localQueue := c.writeQueue
		c.writeQueue = make([]string, 0)
		go func(c *Client, queue []string) {
			for _, message := range queue {
				c.socketSend <- message
			}
		}(c, localQueue)
	}

	for {
		if c.socket == nil {
			return
		}
		message = ""
		message = <-c.socketSend
		err := websocket.Message.Send(c.socket, message)
		if err != nil {
			// If we got an error writing this message, then place it in the emergency message queue before
			// exiting.  This will ensure that a message that would be lost by "returning" here will be
			// picked up and re-attempted upon entry to this function before the recieve loop
			c.writeQueue = append(c.writeQueue, message)
			c.log("simperium.Client.mindSocketWrites websocket.Message.Send error: %s", err.Error())
			return
		}
		c.log(">>> %s", message)
	}
}

func (c *Client) mindSocketReads() {
	// The read can panic
	defer func() { recover() }()
	var message string
	for {
		if c.socket == nil {
			return
		}
		err := websocket.Message.Receive(c.socket, &message)
		c.lastReadAt = time.Now()
		if err != nil {
			c.log("simperium.Client.mindSocketWrites websocket.Message.Send error: %s", err.Error())
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

func (c *Client) ConnectTo(wssUrl string) error {
	if c.wssUrl == "" && wssUrl == "" {
		wssUrl = "wss://api.simperium.com:443/sock/websocket"
	}
	c.wssUrl = wssUrl
	c.clientId = uuid.New()
	if false == c.initialized {
		if 0 == c.heartbeat {
			c.SetHeartbeat(time.Second)
		}
		c.socketRecv = make(chan string)
		c.socketSend = make(chan string)
		c.recvChans = make(map[int]chan string)
		c.recvCount = make(map[int]func())
		c.socketError = make(chan error)
		c.connectedClient = make(chan bool)
		c.buckets = make(map[string]*Bucket)
		c.writeQueue = make([]string, 0)
	}
	cfg, err := websocket.NewConfig(c.wssUrl, "https://github.com/apokalyptik/go-simperium")
	if err != nil {
		return err
	}
	if parsedUrl, err := url.Parse(c.wssUrl); err == nil {
		if strings.HasSuffix(parsedUrl.Host, ".simperium.com:443") {
			// For development, different hostname but same ssl cert
			cfg.TlsConfig = &tls.Config{InsecureSkipVerify: true}
		}
	} else {
		return err
	}
	cfg.Header.Add("Host", "api.simperium.com")
	socket, err := websocket.DialConfig(cfg)
	if err != nil {
		return err
	}
	c.socket = socket
	c.connectedAt = time.Now()
	if false == c.initialized {
		go c.mindSocketReadWrite()
		go c.handleSocketReads()
		go c.mindHeartbeats()
		go c.mindDisconnects()
		c.initialized = true
	} else {
		c.connectedClient <- true // Wake reader and writer again
		for _, bucket := range c.buckets {
			bucket.reconnect()
		}
	}
	return nil
}

func (c *Client) Connect() error {
	return c.ConnectTo(c.wssUrl)
}
