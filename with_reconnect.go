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
	isConnected     MutexWithBool
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
	o.isConnected.Lock()
	errCh := make(chan error)
	const MaxErrorGoroutines = 5
	s := newChanErrorSenderWithCap(MaxErrorGoroutines, errCh)
	go func() {
		for {
			if !o.isConnected.IsLocked() {
				o.isConnected.Lock()
			}

			connection, err := amqp.DialConfig(url, config)
			if err != nil {
				s.Send(fmt.Errorf("dial: %v", err))
				o.sleeper.Inc()
				o.sleeper.Sleep()
				continue
			}
			newChannel, err := connection.Channel()
			if err != nil {
				s.Send(fmt.Errorf("channel: %v", err))
				o.sleeper.Inc()
				o.sleeper.Sleep()
				continue
			}
			o.sleeper.Drop()

			o.channelMx.Lock()
			o.currentChannel = newChannel
			o.channelMx.Unlock()

			o.isConnected.Unlock()
			notifyClose := make(chan *amqp.Error)
			s.Send(fmt.Errorf("notify: %v", <-connection.NotifyClose(notifyClose)))
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
) (*chan Event, <-chan error, chan<- bool) {
	exchangeCfg, queueCfg, queueBindCfg, consumeCfg = initSubscribeConfigs(exchangeCfg, queueCfg, queueBindCfg, consumeCfg)
	o.waitForConnection()

	outCh := make(chan Event)
	errCh := make(chan error)
	doneCh := make(chan bool, 1)
	errSender := newChanErrorSenderWithCap(o.reconnectParams.DelayCap, errCh)

	go o.listener(
		exchangeName,
		outCh,
		doneCh,
		errSender,
		reflect.Indirect(reflect.ValueOf(reply)).Interface(),
		exchangeCfg,
		queueCfg,
		queueBindCfg,
		consumeCfg,
	)

	return &outCh, errCh, doneCh
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

func (o *observerWithReconnect) listener(
	exchangeName string,
	outCh chan<- Event,
	doneCh <-chan bool,
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
			return
		default:
			queueName, err = o.declareQueue(exchangeName, exchangeCfg, queueCfg, queueBindCfg, consumeCfg)
			if err != nil {
				errorSender.Send(err)
				o.sleeper.Inc()
				o.sleeper.Sleep()
				continue
			}
			deliveryCh, err := channelConsume(o.currentChannel, queueName, consumeCfg)
			if err != nil {
				errorSender.Send(fmt.Errorf("consume: %v", err))
				o.sleeper.Inc()
				o.sleeper.Sleep()
				continue
			}
			o.sleeper.Drop()
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
	if o.isConnected.IsLocked() {
		o.isConnected.Lock()
		o.isConnected.Unlock()
	}
	return
}
