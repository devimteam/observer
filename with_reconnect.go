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
	o.connection.Disconnected()
	go o.reconnectLoop(errCh)
	return &o, errCh
}

func (o *observerWithReconnect) reconnectLoop(errCh chan<- error) {
	const SenderBuffer = 5
	s := newLogger(o.options.loggerLevel, SenderBuffer, errCh)
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
	defer channel.Close()
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
	reporter := newLogger(o.options.loggerLevel, o.options.timeoutCap, errCh)
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
	logger *logger,
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
				logger.Log(0, err)
				continue
			}
		}
		channel, err := o.connection.conn.Channel()
		if err != nil {
			logger.Log(0, err)
			continue
		}
		notifier := channel.NotifyClose(make(chan *amqp.Error))
		deliveryCh, queueName, err := prepareDeliveryChan(channel, exchangeName, exchangeCfg, queueCfg, queueBindCfg, consumeCfg, logger)
		if err != nil {
			logger.Log(0, err)
			continue
		}

	DeliveryLoop:
		for {
			select {
			case d := <-deliveryCh:
				ev, err := o.handleEvent(d, reply)
				if err != nil {
					e := d.Nack(false, true)
					if e != nil {
						logger.Log(1, fmt.Errorf("nack: %v", e))
					}
					logger.Log(1, fmt.Errorf("handle event: %v", err))
					continue
				}
				outCh <- ev
			case <-notifier:
				logger.Log(0, fmt.Errorf("delivery channel was closed"))
				break DeliveryLoop
			case <-doneCh:
				close(outCh)
				close(doneCh)
				if channel != nil {
					channel.QueueUnbind(queueName, queueBindCfg.Key, exchangeName, queueBindCfg.Args)
					channel.QueueDelete(queueName, queueCfg.IfUnused, queueCfg.IfEmpty, queueCfg.NoWait)
					channel.Close()
				}
				logger.Log(1, fmt.Errorf("done"))
				return
			}
		}
	}
}

func prepareDeliveryChan(
	channel *amqp.Channel,
	exchangeName string,
	exchangeCfg *ExchangeConfig,
	queueCfg *QueueConfig,
	queueBindCfg *QueueBindConfig,
	consumeCfg *ConsumeConfig,
	reporter *logger,
) (<-chan amqp.Delivery, string, error) {
	reporter.Log(3, fmt.Errorf("prepare delivery chan for exchange %s", exchangeName))
	reporter.Log(3, fmt.Errorf("exchange(%s) declare", exchangeName))
	err := channelExchangeDeclare(channel, exchangeName, exchangeCfg)
	if err != nil {
		return nil, "", fmt.Errorf("exchange declare err: %v", err)
	}
	reporter.Log(3, fmt.Errorf("queue(%s) declare", queueCfg.Name))
	q, err := channelQueueDeclare(channel, queueCfg)
	if err != nil {
		return nil, "", fmt.Errorf("queue declare err: %v", err)
	}
	reporter.Log(3, fmt.Errorf("bind queue(%s) to exchange(%s)", q.Name, exchangeName))
	err = channelQueueBind(channel, q.Name, exchangeName, queueBindCfg)
	if err != nil {
		return nil, "", fmt.Errorf("queue bind err: %v", err)
	}
	reporter.Log(3, fmt.Errorf("consume from queue(%s)", q.Name))
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
