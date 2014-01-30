go-simperium
============

usage

```go
import "github.com/apokalyptik/go-simperium"

//...

func handleBucketReady(bucketname string) {
	//...
}

func handleBucketNotifyInit(bucketname, documentid string, data map[string] interface{}) {
	//...
}

func handleBucketNotify(bucketname, documentid string, data map[string] interface{}) {
	//...
}

func handleBucketLocal(bucketname, documentid string) data map[string] interface{} {
	//...
}

//...

client := new(simperium.Client)
bucket, err := client.Bucket(appid, bucketname, usertoken)
if err != nil {
	log.Fatal(err)
}
bucket.OnLocal(handleBucketLocal)
bucket.OnReady(handleBucketReady)
bucket.OnNotifyInit(handleBucketNotifyInit)
bucket.OnNotify(handleBucketNotify)
bucket.Start()
```
