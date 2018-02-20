package observer

import (
	"sync"

	"github.com/streadway/amqp"
)

type pubChanPool struct {
	once    bool
	channel *safeChannel
}

func newPubChanPool(once bool) *pubChanPool {
	return &pubChanPool{once: once, channel: &safeChannel{}}
}

func (p pubChanPool) Channel(conn *amqp.Connection) (*amqp.Channel, error) {
	if p.once && p.channel.ch != nil {
		return p.channel.ch, nil
	}
	return p.newChannel(conn)
}

func (p pubChanPool) newChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if p.once {
		p.channel.Set(ch)
		go func() {
			if p.channel.ch != nil {
				<-p.channel.ch.NotifyClose(make(chan *amqp.Error))
				p.channel.Set(nil)
			}
		}()
	}
	return ch, err
}

type safeChannel struct {
	ch *amqp.Channel
	sync.Mutex
}

func (c *safeChannel) Set(ch *amqp.Channel) {
	c.Lock()
	defer c.Unlock()
	c.ch = ch
}
