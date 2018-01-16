// fixme: Not compilable test
package test

import (
	"testing"
	"time"

	"github.com/devimteam/gounit"
	"github.com/devimteam/observer"
	"github.com/devimteam/observer/json"
	"github.com/devimteam/observer/proto"
	"github.com/devimteam/observer/test/pb"
	"github.com/streadway/amqp"
)

type EventResult struct {
	Name string `json:"name"`
}

func TestJSONClient(t *testing.T) {
	u := gounit.New(t)

	service := "test"

	conn, err := amqp.Dial("amqp://guest:guest@localhost:32769/")
	u.AssertNotError(err, gounit.EmptyMessage)

	ch, err := conn.Channel()
	u.AssertNotError(err, gounit.EmptyMessage)

	observ := observer.New(ch, json.NewCodec())

	doneCh := make(chan observer.Event)

	go func() {
		chOut, err := observ.Sub(service, &EventResult{})
		u.AssertNotError(err, gounit.EmptyMessage)

		e := <-chOut

		doneCh <- e
	}()

	time.Sleep(3 * time.Second)

	err = observ.Pub(service, &EventResult{Name: "Pegas"})
	u.AssertNotError(err, gounit.EmptyMessage)

	e := <-doneCh

	u.AssertEquals(e.Data.(*EventResult).Name, "Pegas", gounit.EmptyMessage)
}

func TestProtoClient(t *testing.T) {
	u := gounit.New(t)

	service := "test"

	conn, err := amqp.Dial("amqp://guest:guest@localhost:32769/")
	u.AssertNotError(err, gounit.EmptyMessage)

	ch, err := conn.Channel()
	u.AssertNotError(err, gounit.EmptyMessage)

	observ := observer.New(ch, proto.NewCodec())

	doneCh := make(chan observer.Event)

	go func() {
		chOut, err := observ.Sub(service, &pb.EventResult{})
		u.AssertNotError(err, gounit.EmptyMessage)

		e := <-chOut

		doneCh <- e
	}()

	time.Sleep(3 * time.Second)

	err = observ.Pub(service, &pb.EventResult{Name: "Pegas"})
	u.AssertNotError(err, gounit.EmptyMessage)

	e := <-doneCh

	u.AssertEquals(e.Data.(*pb.EventResult).Name, "Pegas", gounit.EmptyMessage)
}
