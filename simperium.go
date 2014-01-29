package gosimperium

import(
	"code.google.com/p/go.net/websocket"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"log"
	"fmt"
	"io"
	"bytes"
	"errors"
)

var empty string = "000000000000000000000000"

var authFailMsg []byte = []byte("0:auth:expired")
var AUTHERR error = errors.New("Authorization Failed")

type Index map[string] Document

type Document map[string] interface{}

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
	if err := c.sendString(fmt.Sprintf("0:init:%s", string(jsonText))); err != nil {
		c.socket = nil
		return err
	}
	if _, err = c.read(); err != nil {
		return err
	}
	if err := c.sendString("0:cv:"); err != nil {
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
