package simperium

// TODO: auth failure on *first* bucket disconnects socket... which has been difficule to rectify... need to look further into handling
// this particular error

import(
	"code.google.com/p/go.net/websocket"
	"code.google.com/p/go-uuid/uuid"
	"bytes"
	"log"
	"fmt"
	"time"
	"sync"
	"regexp"
	"strconv"
	"io"
	"strings"
)

var channelMessage *regexp.Regexp = regexp.MustCompile("^\\d+:")

type Client struct {
	clientId string
	socket *websocket.Conn

	socketError chan error
	socketRecv chan string
	socketSend chan string

	buckets map[string] *Bucket
	bucketId []*Bucket
	recvChans []chan string
	recvCount []func()
	channels int

	lock sync.Mutex
}

func (c *Client) Bucket(app, name, token string) (*Bucket, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.socket == nil {
		c.connect()
	}
	key := fmt.Sprintf("%s:%s:%s", name, token, token)
	if bucket, ok := c.buckets[key]; ok {
		return bucket, nil
	}
	bucket := new(Bucket)
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
		var prepend = fmt.Sprintf("%d:",channel)
		for {
			m := <-bucket
			client<- prepend + m
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
	tick := time.Tick(time.Duration(15*time.Second))
	count := 0
	for {
		<-tick
		c.socketSend<- fmt.Sprintf("h:%d",count)
		count++
	}
}

func (c *Client) mindSocketWrites() {
	for {
		b := <-c.socketSend
		_, err := c.socket.Write([]byte(b))
		log.Printf(">>> %s", b)
		if err != nil {
			c.socketError<- err
			return
		}
	}
}

func (c *Client) mindSocketReads() {
	var buf bytes.Buffer
	for {
		chunk := make([]byte,1024)
		_, err := c.socket.Read(chunk);
		chunk = bytes.TrimRight(chunk, "\000")
		if len(chunk) > 0 {
			if _, buferr := buf.Write(chunk); buferr != nil {
				log.Printf("simperium.Client.mindSocketReads buffer write error: %s", err.Error())
				c.socketError<- buferr
				return
			}
		}
		if buf.Len()%1024 != 0 {
			c.socketRecv<-buf.String()
			log.Printf("<<< %s", buf.String())
			buf.Truncate(0)
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("simperium.Client.mindSocketReads read error: %s", err.Error())
				c.socketError<- err
				return
			}
		}
	}
}

func (c *Client) sendChannel(channel int, m string) {
	c.recvChans[channel]<- m
}

func (c *Client) handleSocketReads() {
	var hb string = "h:"
	var chanCache = make(map[string]int)
	for {
		m := <-c.socketRecv
		switch {
			case strings.HasPrefix(m, hb):
				break;
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
				break;
			default:
				log.Printf("unknown socket read: %s", string(m))
		}
	}
}

func (c *Client) connect() error {
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

/*
type Client struct {
	index Index
	recv chan []byte
	err chan error
	sendS chan string
	sendB chan []byte
	done chan bool
	socket *websocket.Conn
	buffer bytes.Buffer
	clientid string
	appid string
	bucket string
	utoken string
	debug bool
	channel int
}

func (c *Client) connect() error {
	c.index = make(Index)
	c.recv = make(chan []byte)
	c.err = make(chan error)
	socket, err := websocket.Dial(
		"wss://api.simperium.com:443/sock/websocket",
		"",
		"https://github.com/apokalyptik/go-simperium")
	if err != nil {
		return err
	}
	c.socket = socket
	return c.auth()
}

func (c *Client) Debug(setting bool) {
	c.debug = setting
}

func (c *Client) log(s string) {
	if c.debug {
		log.Printf(s)
	}
}

func (c *Client) read() ([]byte, error) {
	c.buffer.Truncate(0)
	for {
		buf := make([]byte,1024)
		if _, err := c.socket.Read(buf); err != nil {
			if err != io.EOF {
				return make([]byte,0), err
			}
		}
		c.buffer.Write(bytes.TrimRight(buf, "\000"))
		if c.buffer.Len()%1024 != 0 {
			break
		}
	}
	rval := c.buffer.Bytes()
	c.log(fmt.Sprintf("<<< %s", string(rval)))
	if 0 == bytes.Compare(rval, authFailMsg) {
		return make([]byte,0), AUTHERR
	}
	return rval, nil
}

func (c *Client) send(b []byte) error {
	c.log(fmt.Sprintf(">>> %s", string(b)))
	_, e := c.socket.Write(b)
	return e
}

func (c *Client) sendString(s string) error {
	return c.send([]byte(s))
}

func (c *Client) auth() error {
	jsonText, err := json.Marshal(map[string]interface{}{
		"app_id": c.appid,
		"token": c.utoken,
		"name": c.bucket,
		"clientid": c.clientid,
		"library": "github.com/apokalyptik/go-simperium",
		"library_version": "1",
		"api": 1})
	if err != nil {
		return err
	}
	if err := c.sendString(fmt.Sprintf("%d:init:%s", c.channel, string(jsonText))); err != nil {
		c.socket = nil
		return err
	}
	if _, err = c.read(); err != nil {
		return err
	}
	if err := c.sendString(fmt.Sprintf("%d:cv:", c.channel)); err != nil {
		return err
	}
	if _, err := c.read(); err != nil {
		return err
	}
	go c.handle()
	return nil
}

func (c *Client) handle() {
	c.done = make(chan bool, 1)
	inChan := make(chan []byte)
	errChan := make(chan error)
	go func(c *Client, inChan chan []byte, errChan chan error) {
		for {
			if buf, err := c.read(); err != nil {
				errChan<- err
			} else {
				inChan<- buf
			}
		}
	}(c, inChan, errChan)
	for {
		select {
			case m := <-inChan:
				c.recv<- m
			case e := <-errChan:
				c.err<- e
			case s := <-c.sendS:
				c.socket.Write([]byte(s))
			case b := <-c.sendB:
				c.socket.Write(b)
		}
	}
}

func DialDebug(appid, bucket, utoken string) (*Client, error) {
	client := new(Client)
	client.appid = appid
	client.bucket = bucket
	client.utoken = utoken
	client.Debug(true)
	return initClient(client)
}

func Dial(appid, bucket, utoken string) (*Client, error) {
	client := new(Client)
	client.appid = appid
	client.bucket = bucket
	client.utoken = utoken
	return initClient(client)
}

func initClient(client *Client) (*Client, error) {
	client.clientid = uuid.NewUUID().String()
	if err := client.connect(); err != nil {
		return nil, err
	}
	return client, nil
}
*/


