package observer

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type observerWithReconnect struct {
	url    string
	config amqp.Config
	codec  Codec

	channelMx      sync.Mutex
	currentChannel *amqp.Channel

	reconnectParams *ReconnectParams
	sleeper         *sleeper
	reconnectMx     sync.Mutex
}

type ReconnectParams struct {
	DelayBase time.Duration
	DelayCap  int
}

func WithReconnect(codec Codec, url string, config amqp.Config, reconnectParams ReconnectParams) (Observer, <-chan error) {
	o := observerWithReconnect{
		codec:           codec,
		config:          config,
		url:             url,
		sleeper:         NewSleeper(reconnectParams.DelayCap, reconnectParams.DelayBase),
		reconnectParams: &reconnectParams,
	}
	errCh := make(chan error)
	const SenderBuffer = 5
	s := newChanErrorSenderWithCap(SenderBuffer, errCh)
	o.reconnectMx.Lock()
	go func() {
		for {
			connection, newChannel := o.infiniteReconnect(url, config, s)
			o.sleeper.Drop()
			o.updateCurrentChannel(newChannel)
			o.reconnectMx.Unlock()

			notifyClose := make(chan *amqp.Error)
			s.Send(fmt.Errorf("connection closed: %v", <-connection.NotifyClose(notifyClose)))
			o.reconnectMx.Lock()
		}
	}()
	return &o, errCh
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
	o.waitForConnection()

	outCh := make(chan Event)
	errCh := make(chan error)
	doneCh := make(chan bool, 1)
	errSender := newChanErrorSenderWithCap(o.reconnectParams.DelayCap, errCh)
	retlyType := reflect.Indirect(reflect.ValueOf(reply)).Interface()

	go o.listener(
		exchangeName,
		outCh, doneCh, errSender,
		retlyType,
		exchangeCfg, queueCfg, queueBindCfg, consumeCfg,
	)

	return outCh, errCh, doneCh
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
	o.waitForConnection()
	err := channelExchangeDeclare(o.currentChannel, exchangeName, exchangeCfg)
	if err != nil {
		return fmt.Errorf("exchange declare err: %v", err)
	}

	resp := o.codec.NewResponse(data)
	dataBytes, err := resp.Body()
	if err != nil {
		return err
	}
	o.waitForConnection()
	err = channelPublish(o.currentChannel, exchangeName, publishCfg,
		amqp.Publishing{
			ContentType: o.codec.ContentType(),
			Body:        dataBytes,
		})
	if err != nil {
		return fmt.Errorf("publish err: %v", err)
	}
	return nil
}

func (o *observerWithReconnect) infiniteReconnect(url string, config amqp.Config, errorSender *chanErrorSenderWithCap) (connection *amqp.Connection, newChannel *amqp.Channel) {
	var err error
	for {
		o.sleeper.Sleep()
		o.sleeper.Inc()
		connection, err = amqp.DialConfig(url, config)
		if err != nil {
			errorSender.Send(fmt.Errorf("dial: %v", err))
			continue
		}
		newChannel, err = connection.Channel()
		if err != nil {
			errorSender.Send(fmt.Errorf("channel: %v", err))
			continue
		}
		break // successfully connected
	}
	return connection, newChannel
}

func (o *observerWithReconnect) updateCurrentChannel(ch *amqp.Channel) {
	o.channelMx.Lock()
	o.currentChannel = ch
	o.channelMx.Unlock()
}

func (o *observerWithReconnect) listener(
	exchangeName string,
	outCh chan<- Event,
	doneCh chan bool,
	errorSender *chanErrorSenderWithCap,
	reply interface{},
	exchangeCfg *ExchangeConfig,
	queueCfg *QueueConfig,
	queueBindCfg *QueueBindConfig,
	consumeCfg *ConsumeConfig,
) {
	var (
		queueName string
		err       error
	)
	defer func() {
		o.currentChannel.QueueUnbind(queueName, queueBindCfg.Key, exchangeName, queueBindCfg.Args)
		o.currentChannel.QueueDelete(queueName, queueCfg.IfUnused, queueCfg.IfEmpty, queueCfg.NoWait)
	}()
	for {
		select {
		case <-doneCh:
			close(outCh)
			close(doneCh)
			return
		default:
			o.waitForConnection()
			queueName, err = o.declareQueue(exchangeName, exchangeCfg, queueCfg, queueBindCfg, consumeCfg)
			if err != nil {
				errorSender.Send(err)
				continue
			}
			deliveryCh, err := channelConsume(o.currentChannel, queueName, consumeCfg)
			if err != nil {
				errorSender.Send(fmt.Errorf("consume: %v", err))
				continue
			}
			for d := range deliveryCh {
				ev, err := o.handleEvent(d, reply)
				if err != nil {
					errorSender.Send(fmt.Errorf("handle event: %v", err))
					continue
				}
				outCh <- ev
			}
			errorSender.Send(fmt.Errorf("delivery channel was closed"))
		}
	}
}

func (o *observerWithReconnect) declareQueue(
	exchangeName string,
	exchangeCfg *ExchangeConfig,
	queueCfg *QueueConfig,
	queueBindCfg *QueueBindConfig,
	consumeCfg *ConsumeConfig,
) (string, error) {
	err := channelExchangeDeclare(o.currentChannel, exchangeName, exchangeCfg)
	if err != nil {
		return "", fmt.Errorf("exchange declare err: %v", err)
	}
	q, err := channelQueueDeclare(o.currentChannel, queueCfg)
	if err != nil {
		return "", fmt.Errorf("queue declare err: %v", err)
	}
	err = channelQueueBind(o.currentChannel, q.Name, exchangeName, queueBindCfg)
	if err != nil {
		return "", fmt.Errorf("queue bind err: %v", err)
	}
	return q.Name, nil
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

func (o *observerWithReconnect) waitForConnection() {
	o.reconnectMx.Lock()
	o.reconnectMx.Unlock()
}
