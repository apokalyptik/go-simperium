package simperium

// BUG(apokalyptik) Buckets do not yet setup an initial internal state

// BUG(apokalyptik) Buckets do not yet maintain their internal state

// BUG(apokalyptik) Buckets to not sync streamed changes

// BUG(apokalyptik) Buckets do not yet send changes

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apokalyptik/go-jsondiff"
	"log"
	"regexp"
	"sync"
	"time"
)

var ErrorAuthFail error = errors.New("Authorization Failed")
var BadServerResponse error = errors.New("Simperium gave an unexpected response")

var authFail *regexp.Regexp = regexp.MustCompile("^auth:expired$")

type bucketItem struct {
	Data    map[string]interface{} `json:"d"`
	Version int        `json:"v"`
	Id      string     `json:"id"`
}

type indexResponse struct {
	Current string       `json:"current"`
	Index   []bucketItem `json:"index"`
	Mark    string       `json:"mark"`
}

// The function signature for the OnReady callback
//
// func Handler(bucket string) {...}
type ReadyFunc func(string)

// The function signature for the OnNotify and OnNotifyInit callbacks
//
// func Handler(bucket, documentId string, data map[string]interface{}) {...}
type NotifyFunc func(string, string, map[string]interface{})

// The function signature for the OnLocal callback
//
// func Handler(bucket, documentId string)  map[string]interface{} {...}
type LocalFunc func(string, string) map[string]interface{}

// The function signature for the OnError callback.
//
// func Handler(bucket string, err error) {...}
type ErrorFunc func(string, error)

type Bucket struct {
	app            string                // The application id
	name           string                // The bucket name
	token          string                // The user auth token
	clientId       string                // The client ID
	isReady        sync.WaitGroup        // Whether or not the client is "ready"
	indexing       bool                  // Whether or not we are doing our index sync
	indexed        bool                  // Whether or not we have completed our index sync
	recv           chan string           // For recieving data from the client
	recvIndex      chan string           // For internally separating out index responses from the stream
	recvChange     chan string           // For internally separating out changeset notifications from the stream
	send           chan string           // For sending commands through the client
	messages       uint64                // The number of messages pending at the client for this bucket
	ready          ReadyFunc             // Callback
	notify         NotifyFunc            // Callback
	notifyInit     NotifyFunc            // Callback
	local          LocalFunc             // Callback
	err            ErrorFunc             // Callback
	waitingChanges []string              // changes awaiting processing
	changesLock    sync.Mutex
	data           map[string]bucketItem // Our index
	debug          bool                  // Whether we're debugging or not
	jsd            *jsondiff.JsonDiff    // Jsondiff
	lock           sync.Mutex            // A mutex
	processLock    sync.Mutex
}

func (b *Bucket) handleRecv() {
	for {
		m := <-b.recv
		b.readMessage()
		if m[:2] == "i:" {
			b.recvIndex <- m[2:]
			continue
		}
		if m[:2] == "c:" {
			b.recvChange <- m[2:]
			continue
		}
		log.Printf("Unhandled message sent to bucket: %s", m)
	}
}

func (b *Bucket) updateDocument(id string, v int, n map[string]interface{}) {
	if n == nil {
		if _, ok := b.data[id]; ok == true {
			delete(b.data, id)
		}
	}
	b.data[id] = bucketItem{ Data: n, Version: v, Id: id }
}
func (b *Bucket) handleChanges() {
	var m string
	b.isReady.Wait()
	for {
		if len(b.waitingChanges) > 0 {
			// Shift an element off the beginning of the slice (fifo)
			b.changesLock.Lock()
			m, b.waitingChanges = b.waitingChanges[0], b.waitingChanges[1:]
			b.changesLock.Unlock()
			// The data is a json ilst of change objects...
			if changes, err := jsondiff.Parse(m); err != nil {
				log.Printf("Got invalid changeset from Simerperium Server: %s", err.Error())
				continue
			} else {
				for _, change := range changes {
					_, ok := b.data[change.Document]

					if true == ok {
						n, e := b.jsd.Apply(b.data[change.Document].Data, change)
						if e != nil {
							log.Printf("Patching error: %s", err.Error())
						} else {
							b.updateDocument(change.Document, change.Resultrevision, n)
						}
					} else {
						n, e := b.jsd.Apply(nil, change)
						if e != nil {
							log.Printf("Patching error: %s", err.Error())
						} else {
							b.updateDocument(change.Document, change.Resultrevision, n)
						}
					}
				}
			}
		} else {
			time.Sleep(time.Duration(100*time.Millisecond))
		}
	}
}

func (b *Bucket) handleIncomingChanges() {
	for {
		// Populate a queue so that we don't fill up the recieving channel and block while
		// processing large indexes on startup for a busy bucket
		c := <-b.recvChange
		b.changesLock.Lock()
		b.waitingChanges = append(b.waitingChanges, c)
		b.changesLock.Unlock()
	}
}

func (b *Bucket) Debug(debug bool) {
	b.debug = debug
}

func (b *Bucket) log(data ...interface{}) {
	if b.debug {
		switch len(data) {
		case 0:
			return
		case 1:
			log.Printf(data[0].(string))
		case 2:
			log.Printf(data[0].(string), data[1:]...)
		}
	}
}

func (b *Bucket) newMessage() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.messages++
}

func (b *Bucket) readMessage() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.messages--
}

func (b *Bucket) drain() {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.messages < 1 {
		b.log("No messages to drain: %d", b.messages)
		return
	}
	for i := b.messages; b.messages > 0; i-- {
		b.log("Draining messages... %d left...", i)
		<-b.recv
		b.messages--
	}
}

func (b *Bucket) read() string {
	defer b.readMessage()
	return <-b.recv
}

// Specify which function to use as a callback to let you know that the startup
// phase of the bucket has finished and the Bucket structure is up, and syncing
func (b *Bucket) OnReady(f ReadyFunc) {
	b.ready = f
}

// Specify which function to use as a callback after the startup phase of the
// bucket to notify you of existing documents and their data
func (b *Bucket) OnNotify(f NotifyFunc) {
	b.notify = f
}

// Specify which function to use as a callback during the startup phase of the
// bucket to notify you of existing documents and their data
func (b *Bucket) OnNotifyInit(f NotifyFunc) {
	b.notifyInit = f
}

// Speficy which function to use as a callback for when the bucket needs to know
// what data a document contains now
func (b *Bucket) OnLocal(f LocalFunc) {
	b.local = f
}

// Specify whch function to use as a callback for when an error is encountered with
// bucket operations
func (b *Bucket) OnError(f ErrorFunc) {
	b.err = f
}

// Tell the bucket that you have new data for the document. The bucket will call
// your OnLocal handler to retrieve the data
func (b *Bucket) Update(documentId string) {
}

// Update or create the document in the bucket to contain the new data
func (b *Bucket) UpdateWith(documentId string, data map[string]interface{}) {
}

func (b *Bucket) init() {
	b.isReady.Add(1)
	b.recvIndex = make(chan string, 1024)
	b.recvChange = make(chan string, 1024)
	b.data = make(map[string]bucketItem)
	b.jsd = jsondiff.New()
}

func (b *Bucket) index() {
	b.processLock.Lock()
	defer b.processLock.Unlock()

	b.indexed = false
	b.indexing = true

	data := "1"
	offset := ""
	since := ""
	limit := "100"

	for {
		b.send <- fmt.Sprintf("i:%s:%s:%s:%s", data, offset, since, limit)
		rdata := <-b.recvIndex
		resp := new(indexResponse)
		if err := json.Unmarshal([]byte(rdata), &resp); err != nil {
			log.Fatal(err)
		}
		for _, v := range resp.Index {
			b.data[v.Id] = v
			if b.notifyInit != nil {
				go b.notifyInit(b.name, v.Id, v.Data)
			}
		}
		if resp.Mark == "" {
			break
		}
		if resp.Mark == offset {
			break
		}
		offset = resp.Mark
	}
	b.indexing = false
	b.indexed = true
}

func (b *Bucket) Start() {
	b.index()
	if b.ready != nil {
		go b.ready(b.name)
	}
	b.isReady.Done()
}

func (b *Bucket) auth() error {
	init, err := json.Marshal(map[string]interface{}{
		"app_id":          b.app,
		"token":           b.token,
		"name":            b.name,
		"clientid":        b.clientId,
		"library":         "github.com/apokalyptik/go-simperium",
		"library_version": "1",
		"api":             1})
	if err != nil {
		return err
	}
	b.send <- "init:" + string(init)
	resp := b.read()
	if authFail.MatchString(resp) {
		return ErrorAuthFail
	}
	go b.handleRecv()
	go b.handleIncomingChanges()
	go b.handleChanges()
	return nil
}
