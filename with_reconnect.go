package observer

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/streadway/amqp"
)

type observerWithReconnect struct {
	codec Codec

	connection  *connectionWrapper
	pubProvider *pubChanPool
	options     options
}

func WithReconnect(codec Codec, url string, config amqp.Config, opts ...Option) (Observer, <-chan error) {
	ops := defaultOptions()
	for _, op := range opts {
		op(&ops)
	}
	o := observerWithReconnect{
		codec:       codec,
		options:     ops,
		connection:  newConnectionWrapper(url, config, NewSleeper(ops.timeoutCap, ops.timeoutBase)),
		pubProvider: newPubChanPool(ops.pubUseOneChannel),
	}
	errCh := make(chan error)
	go o.reconnectLoop(errCh)
	return &o, errCh
}

func (o *observerWithReconnect) reconnectLoop(errCh chan<- error) {
	const SenderBuffer = 5
	s := newReporter(SenderBuffer, errCh)
	o.connection.restore(s)
	return
}

func (o *observerWithReconnect) Pub(
	exchangeName string,
	data interface{},
	exchangeCfg *ExchangeConfig,
	publishCfg *PublishConfig,
) error {
	if exchangeCfg == nil {
		exchangeCfg = DefaultExchangeConfig()
	}
	if publishCfg == nil {
		publishCfg = DefaultPublishConfig()
	}
	if o.options.waitConnection {
		err := o.connection.Wait(o.options.waitConnectionDeadline)
		if err != nil {
			return err
		}
	}
	channel, err := o.pubProvider.Channel(o.connection.conn)
	if err != nil {
		return fmt.Errorf("create channel: %v", err)
	}
	err = channelExchangeDeclare(channel, exchangeName, exchangeCfg)
	if err != nil {
		return fmt.Errorf("exchange declare err: %v", err)
	}

	msg, err := NewSimpleMessage(o.codec, data)
	if err != nil {
		return err
	}
	err = channelPublish(channel, exchangeName, publishCfg, msg)
	if err != nil {
		return fmt.Errorf("publish err: %v", err)
	}
	return nil
}

func (o *observerWithReconnect) Sub(
	exchangeName string,
	reply interface{},
	exchangeCfg *ExchangeConfig,
	queueCfg *QueueConfig,
	queueBindCfg *QueueBindConfig,
	consumeCfg *ConsumeConfig,
) (<-chan Event, <-chan error, chan<- bool) {
	exchangeCfg, queueCfg, queueBindCfg, consumeCfg = initSubscribeConfigs(exchangeCfg, queueCfg, queueBindCfg, consumeCfg)
	errCh := make(chan error, o.options.subErrorChanBuffer)
	outCh := make(chan Event, o.options.subEventChanBuffer)
	doneCh := make(chan bool, 1)
	reporter := newReporter(o.options.timeoutCap, errCh)
	replyType := reflect.Indirect(reflect.ValueOf(reply)).Interface()

	go o.listener(
		exchangeName,
		outCh, doneCh, reporter,
		replyType,
		exchangeCfg, queueCfg, queueBindCfg, consumeCfg,
	)

	return outCh, errCh, doneCh
}

func (o *observerWithReconnect) listener(
	exchangeName string,
	outCh chan<- Event,
	doneCh chan bool,
	reporter *reporter,
	reply interface{},
	exchangeCfg *ExchangeConfig,
	queueCfg *QueueConfig,
	queueBindCfg *QueueBindConfig,
	consumeCfg *ConsumeConfig,
) {
	for { // channel keep-alive loop
		if o.options.waitConnection {
			err := o.connection.Wait(o.options.waitConnectionDeadline)
			if err != nil {
				reporter.Report(err)
				continue
			}
		}
		channel, err := o.connection.conn.Channel()
		if err != nil {
			reporter.Report(err)
			continue
		}
		notifier := channel.NotifyClose(make(chan *amqp.Error))
		deliveryCh, queueName, err := prepateDeliveryChan(channel, exchangeName, exchangeCfg, queueCfg, queueBindCfg, consumeCfg)
		if err != nil {
			reporter.Report(err)
			continue
		}

	DeliveryLoop:
		for {
			select {
			case <-notifier:
				break DeliveryLoop
			case <-doneCh:
				close(outCh)
				close(doneCh)
				if channel != nil {
					channel.QueueUnbind(queueName, queueBindCfg.Key, exchangeName, queueBindCfg.Args)
					channel.QueueDelete(queueName, queueCfg.IfUnused, queueCfg.IfEmpty, queueCfg.NoWait)
					channel.Close()
				}
				return
			case d := <-deliveryCh:
				ev, err := o.handleEvent(d, reply)
				if err != nil {
					e := d.Nack(false, true)
					if e != nil {
						reporter.Report(fmt.Errorf("nack: %v", e))
					}
					reporter.Report(fmt.Errorf("handle event: %v", err))
					continue
				}
				outCh <- ev
			}
			reporter.Report(fmt.Errorf("delivery channel was closed"))
		}
	}
}

func prepateDeliveryChan(
	channel *amqp.Channel,
	exchangeName string,
	exchangeCfg *ExchangeConfig,
	queueCfg *QueueConfig,
	queueBindCfg *QueueBindConfig,
	consumeCfg *ConsumeConfig,
) (<-chan amqp.Delivery, string, error) {
	err := channelExchangeDeclare(channel, exchangeName, exchangeCfg)
	if err != nil {
		return nil, "", fmt.Errorf("exchange declare err: %v", err)
	}
	q, err := channelQueueDeclare(channel, queueCfg)
	if err != nil {
		return nil, "", fmt.Errorf("queue declare err: %v", err)
	}
	err = channelQueueBind(channel, q.Name, exchangeName, queueBindCfg)
	if err != nil {
		return nil, "", fmt.Errorf("queue bind err: %v", err)
	}
	ch, err := channelConsume(channel, q.Name, consumeCfg)
	if err != nil {
		return nil, "", fmt.Errorf("channel consume err: %v", err)
	}
	return ch, q.Name, nil
}

var differentContentTypeErr = errors.New("different content type")

func (o *observerWithReconnect) handleEvent(d amqp.Delivery, reply interface{}) (ev Event, err error) {
	if d.ContentType != o.codec.ContentType() {
		return ev, differentContentTypeErr
	}

	data := reflect.New(reflect.TypeOf(reply))
	req := o.codec.NewRequest(
		&request{d: d},
	)
	err = req.ReadRequest(data.Interface())
	if err != nil {
		return ev, fmt.Errorf("read request: %v", err)
	}

	return Event{d: d, Data: data.Interface()}, nil
}
