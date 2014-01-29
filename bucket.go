package simperium

import(
	"encoding/json"
	"regexp"
	"log"
	"errors"
	"sync"
	"time"
)

var ErrorAuthFail error = errors.New("Authorization Failed")

var authFail *regexp.Regexp = regexp.MustCompile("^auth:expired$")

type Bucket struct {
	app string
	name string
	token string
	clientId string
	recv chan string
	send chan string
	messages uint64
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

func (b *Bucket) auth() error {
	init, err := json.Marshal(map[string] interface{} {
		"app_id": b.app,
		"token": b.token,
		"name": b.name,
		"clientid": b.clientId,
		"library": "github.com/apokalyptik/go-simperium",
		"library_version": "1",
		"api": 1 })
	if err != nil {
		return err
	}
	b.send<- "init:" + string(init)
	time.Sleep(time.Second)
	resp := b.read()
	log.Printf("[%q]", resp)
	if authFail.MatchString(resp) {
		return ErrorAuthFail
	}
	return nil
}
