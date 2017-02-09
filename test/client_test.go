package test

import (
	"testing"

	"time"

	"github.com/l-vitaly/gounit"
	"github.com/l-vitaly/observer"
	"github.com/l-vitaly/observer/json"
	"github.com/streadway/amqp"
)

type EventResult struct {
	Name string `json:"name"`
}

func TestClient(t *testing.T) {
	u := gounit.New(t)

	eventName := "event.test"

	conn, err := amqp.Dial("amqp://guest:guest@localhost:32769/")
	u.AssertNotError(err, gounit.EmptyMessage)

	ch, err := conn.Channel()
	u.AssertNotError(err, gounit.EmptyMessage)

	observ := observer.New(ch, json.NewCodec())

	doneCh := make(chan observer.Event)

	go func() {
		chOut, err := observ.Sub(eventName, EventResult{})
		u.AssertNotError(err, gounit.EmptyMessage)

		e := <-chOut

		doneCh <- e
	}()

	time.Sleep(3 * time.Second)

	err = observ.Pub(eventName, &EventResult{Name: "Pegas"})
	u.AssertNotError(err, gounit.EmptyMessage)

	e := <-doneCh

	u.AssertEquals(e.Data.(*EventResult).Name, "Pegas", gounit.EmptyMessage)
}
