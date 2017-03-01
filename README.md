RMQ Observer For Pub/Sub Event
============================

Subscribe event - Sub(service string, reply interface{}) (OutCh, error)

Publish event - Pub(service string, data interface{}) error

## Examples

## Sub

``` golang
package main

import (
    "log"
    
    "github.com/streadway/amqp"
    "github.com/l-vitaly/observer"
    "github.com/l-vitaly/observer/json"
)

type CreateUserEvent struct {
  ID   int    `json:"id"`
  Name string `json:"name"`
}

func main() {
    conn, err := amqp.Dial("amqp://rmqhost/")
    if err != nil {
        panic(err)
    }
	ch, err := conn.Channel()
	if err != nil {
	    panic(err)
	}
	observ := observer.New(ch, json.NewCodec(), 3) // 3 workers
    chOut, err := observ.Sub(service, &CreateUserEvent{})
    if err != nil {
   	    panic(err)
   	}
    for e := range chOut {
        log.Printf("ID: %d, User name: %s", e.Data.(*CreateUserEvent).ID, e.Data.(*CreateUserEvent).Name)         
    }   
}
```

## Pub

``` golang
package main

import (
    "log"
    
    "github.com/streadway/amqp"
    "github.com/l-vitaly/observer"
    "github.com/l-vitaly/observer/json"
)

type CreateUserEvent struct {
  ID   int    `json:"id"`
  Name string `json:"name"`
}

func main() {
    conn, err := amqp.Dial("amqp://rmqhost/")
    if err != nil {
        panic(err)
    }
	ch, err := conn.Channel()
	if err != nil {
	    panic(err)
	}
	observ := observer.New(ch, json.NewCodec(), 3) // 3 workers
	err = observ.Pub(service, &CreateUserEvent{ID: 1, Name: "Hello World"})
	if err != nil {
        panic(err)
    }
}
```
