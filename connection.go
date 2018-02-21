package observer

import (
	"errors"
	"sync"
	"time"

	"fmt"

	"github.com/streadway/amqp"
)

type connectionWrapper struct {
	mx      sync.Mutex
	locked  bool
	conn    *amqp.Connection
	url     string
	config  amqp.Config
	sleeper *sleeper

	notifier notifier
}

func newConnectionWrapper(url string, config amqp.Config, sleeper *sleeper) *connectionWrapper {
	cw := connectionWrapper{url: url, config: config, locked: true, sleeper: sleeper}
	return &cw
}

func (s *connectionWrapper) restore(logger *logger) {
	s.Disconnected()
	logger.Log(2, fmt.Errorf("disconnected from %s", s.url))
	for {
		s.sleeper.Sleep()
		s.sleeper.Inc()
		connection, err := amqp.DialConfig(s.url, s.config)
		if err != nil {
			logger.Log(0, fmt.Errorf("dial: %v", err))
			continue
		}
		s.conn = connection
		go func() {
			logger.Log(0, fmt.Errorf("connection closed: %v", <-connection.NotifyClose(make(chan *amqp.Error))))
			s.notifier.notify()
			s.restore(logger)
		}()
		break
	}
	defer s.Connected()
	defer logger.Log(2, fmt.Errorf("connected to %s", s.url))
}

func (s *connectionWrapper) NotifyClose() <-chan signal {
	ch := make(chan signal)
	s.notifier.register(ch)
	return ch
}

func (s *connectionWrapper) Connected() {
	s.mx.Unlock()
	s.locked = true
}

func (s *connectionWrapper) Disconnected() {
	s.mx.Lock()
	s.locked = true
}

func (s *connectionWrapper) IsConnected() bool {
	return s.locked
}

var TimeoutError = errors.New("reached the deadline")

func (s *connectionWrapper) Wait(deadline time.Duration) error {
	r := make(chan struct{})
	go func() {
		defer close(r)
		s.mx.Lock()
		s.mx.Unlock()
		r <- struct{}{}
	}()
	select {
	case <-r:
		return nil
	case <-time.After(deadline):
		return TimeoutError
	}
}

type signal struct{}

type notifier struct {
	mx        sync.Mutex
	receivers []chan<- signal
}

func (d *notifier) register(r chan<- signal) {
	d.mx.Lock()
	defer d.mx.Unlock()
	d.receivers = append(d.receivers, r)
}

func (d *notifier) notify() {
	d.mx.Lock()
	defer d.mx.Unlock()
	for i := range d.receivers {
		d.receivers[i] <- signal{}
		close(d.receivers[i])
	}
	d.receivers = []chan<- signal{}
}
