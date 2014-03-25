/*
A true (persistently connected and stateful) Simperium client

Example Simperium client code:
	package main

	import (
		"github.com/apokalyptik/go-simperium"
		"log"
	)

	var isReady chan bool

	func onReady(bucket string) {
		isReady <- true
	}

	func main() {
		wait := make(chan struct{})
		isReady = make(chan bool, 1)

		simperium := new(simperium.Client)
		simperium.SetDebug(true)
		testBucket, err := simperium.Bucket(myAppID, myBucketName, myApiKey)
		if err != nil {
			log.Fatal(err)
		}
		testBucket.OnReady(onReady)
		testBucket.Start()
		<-isReady
		sendDoc := map[string]interface{}{
			"someString":  "one two three four",
			"someNumbers": []int{1, 2, 3, 4},
			"oneNumber":   1,
			"oneDict":     map[string]interface{}{"one": 1},
			"someDicts":   []map[string]interface{}{{"two": 2}, {"three": 3}}}
		testBucket.Update("document-id", sendDoc)
		<-wait
	}
*/
package simperium
