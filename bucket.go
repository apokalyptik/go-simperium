package simperium

// BUG(apokalyptik) Buckets do not yet setup an initial internal state

// BUG(apokalyptik) Buckets do not yet maintain their internal state

// BUG(apokalyptik) Buckets to not sync streamed changes

// BUG(apokalyptik) Buckets do not yet send changes

import (
	"encoding/json"
	"errors"
	"log"
	"regexp"
	"sync"
	"time"
)

var ErrorAuthFail error = errors.New("Authorization Failed")

var authFail *regexp.Regexp = regexp.MustCompile("^auth:expired$")

// The function signature for the OnReady callback
type ReadyFunc func(string)

// The function signature for the OnNotify and OnNotifyInit callbacks
type NotifyFunc func(string, string, map[string]interface{})

// The function signature for the OnLocal callback
type LocalFunc func(string, string) map[string]interface{}

type Bucket struct {
	app      string
	name     string
	token    string
	clientId string
	recv     chan string
	send     chan string
	messages uint64

	ready      ReadyFunc
	notify     NotifyFunc
	notifyInit NotifyFunc
	local      LocalFunc

	lock sync.Mutex
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
		log.Printf("No messages to drain: %d", b.messages)
		return
	}
	for i := b.messages; b.messages > 0; i-- {
		log.Printf("Draining messages... %d left...", i)
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

// Tell the bucket that you have new data for the document. The bucket will call
// your OnLocal handler to retrieve the data
func (b *Bucket) Update(documentId string) {
}

// Update or create the document in the bucket to contain the new data
func (b *Bucket) UpdateWith(documentId string, data map[string]interface{}) {
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
	time.Sleep(time.Second)
	resp := b.read()
	if authFail.MatchString(resp) {
		return ErrorAuthFail
	}
	return nil
}
