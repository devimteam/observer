package observer

import "github.com/streadway/amqp"

type Request interface {
	ContentType() string
	Body() []byte
}

type request struct {
	d amqp.Delivery
}

func (r *request) ContentType() string {
	return r.d.ContentType
}

func (r *request) Body() []byte {
	return r.d.Body
}
