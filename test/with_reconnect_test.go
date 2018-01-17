package test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/devimteam/observer"
	"github.com/devimteam/observer/json"
	"github.com/streadway/amqp"
)

type args struct {
	codec           observer.Codec
	url             string
	config          amqp.Config
	reconnectParams observer.ReconnectParams
}

type X struct {
	Num int
}

func TestWithReconnect(t *testing.T) {
	tt := args{
		codec: json.NewCodec(),
		url:   fmt.Sprintf("amqp://%s:%d", "localhost", 5672),
		config: amqp.Config{
			SASL: []amqp.Authentication{
				&amqp.PlainAuth{Username: "guest", Password: "guest"},
			},
		},
		reconnectParams: observer.ReconnectParams{
			DelayBase: time.Second,
			DelayCap:  5,
		},
	}
	observer := newTestObserver("reconnect:", tt)
	type X struct {
		Text string
	}
	go func() {
		events, errCh1, err := observer.Sub("test", &X{}, nil, nil, nil, nil)
		if err != nil {
			t.Fatal("subscriber: ", err)
		}
		go func() {
			for {
				fmt.Println("subscriber: ", <-errCh1)
			}
		}()
		for ev := range events {
			fmt.Println("subscriber: ", ev.Data)
		}
	}()
	time.Sleep(time.Second * 1)
	fmt.Println("publisher: start pubing")
	for s := 0; s < 15; s++ {
		err := observer.Pub("test", X{strconv.Itoa(s)}, nil, nil)
		if err != nil {
			fmt.Println("publisher: pub error:", err)
			s--
		}
		fmt.Println("publisher: ", s)
		time.Sleep(time.Second * 3)
	}
	fmt.Println("publisher: done pubing")
}

func TestWithReconnectDifObservers(t *testing.T) {
	tt := args{
		codec: json.NewCodec(),
		url:   fmt.Sprintf("amqp://%s:%d", "localhost", 5672),
		config: amqp.Config{
			SASL: []amqp.Authentication{
				&amqp.PlainAuth{Username: "guest", Password: "guest"},
			},
		},
		reconnectParams: observer.ReconnectParams{
			DelayBase: time.Second,
			DelayCap:  5,
		},
	}
	subscriber := newTestObserver("subscriber: reconnect:", tt)
	publisher := newTestObserver("publisher: reconnect:", tt)
	type X struct {
		Text string
	}
	go func() {
		events, errCh1, err := subscriber.Sub("test", &X{}, nil, nil, nil, nil)
		if err != nil {
			t.Fatal("subscriber: ", err)
		}
		go func() {
			for {
				fmt.Println("subscriber: ", <-errCh1)
			}
		}()
		for ev := range events {
			fmt.Println("subscriber: ", ev.Data)
		}
	}()
	time.Sleep(time.Second * 1)
	fmt.Println("publisher: start pubing")
	for s := 0; s < 15; s++ {
		err := publisher.Pub("test", X{strconv.Itoa(s)}, nil, nil)
		if err != nil {
			fmt.Println("publisher: pub error:", err)
			s--
		}
		fmt.Println("publisher: ", s)
		time.Sleep(time.Second * 3)
	}
	fmt.Println("publisher: done pubing")
}

func newTestObserver(errorPrefix string, tt args) observer.Observer {
	obs, errCh := observer.WithReconnect(tt.codec, tt.url, tt.config, tt.reconnectParams)
	go func() {
		for {
			fmt.Println(errorPrefix, <-errCh)
		}
	}()
	return obs
}

func subFunc(prefix string, obs observer.Observer, quecfg *observer.QueueConfig) {
	events, errCh1, done := obs.Sub("test_wtf", &X{}, nil, quecfg, nil, nil)
	go func() {
		for {
			fmt.Println(prefix, "err:", <-errCh1)
		}
	}()
	for ev := range events {
		fmt.Println(prefix, "data:", ev.Data)
		ev.Commit()
	}
	done <- true
}

func pubFunc(prefix string, from, to int, obs observer.Observer) {
	fmt.Println(prefix, "start pubing")
	for s := from; s < to; s++ {
		err := obs.Pub("test_wtf", X{s}, nil, nil)
		if err != nil {
			fmt.Println(prefix, "pub error:", err)
			s--
		}
		fmt.Println(prefix, s)
		time.Sleep(time.Second * 3)
	}
	fmt.Println(prefix, "done pubing")
}

func TestWithReconnectManyObservers(t *testing.T) {
	tt := args{
		codec: json.NewCodec(),
		url:   fmt.Sprintf("amqp://%s:%d", "localhost", 5672),
		config: amqp.Config{
			SASL: []amqp.Authentication{
				&amqp.PlainAuth{Username: "guest", Password: "guest"},
			},
		},
		reconnectParams: observer.ReconnectParams{
			DelayBase: time.Second,
			DelayCap:  5,
		},
	}
	subscriberA := newTestObserver("sub A: reconnect:", tt)
	subscriberB := newTestObserver("sub B: reconnect:", tt)
	publisherA := newTestObserver("pub A: reconnect:", tt)
	publisherB := newTestObserver("pub B: reconnect:", tt)
	go subFunc("sub A:", subscriberA, nil)
	go subFunc("sub B:", subscriberB, nil)
	time.Sleep(time.Second * 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		pubFunc("pub A:", 0, 10, publisherA)
		wg.Done()
	}()
	go func() {
		pubFunc("pub B:", 50, 60, publisherB)
		wg.Done()
	}()
	wg.Wait()
}
