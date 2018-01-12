package observer

import (
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

	errorWaiter int
}

const MaxErrorGoroutines = 5

func WithReconnect(codec Codec, url string, config amqp.Config) (Observer, chan<- error) {
	o := observerWithReconnect{
		codec:  codec,
		config: config,
		url:    url,
	}
	errCh := make(chan error)
	s := newChanErrorSenderWithCap(MaxErrorGoroutines, errCh)
	sleeper := NewSleeper(10, time.Second)
	go func() {
		for {
			sleeper.Sleep()
			connection, err := amqp.DialConfig(url, config)
			if err != nil {
				s.Send(err)
				sleeper.Increase(1)
				continue
			}
			newChannel, err := connection.Channel()
			if err != nil {
				s.Send(err)
				sleeper.Increase(1)
				continue
			}
			o.channelMx.Lock()
			o.currentChannel = newChannel
			o.channelMx.Unlock()
			sleeper.Drop()
			notifyClose := make(chan *amqp.Error)
			connection.NotifyClose(notifyClose)
		}
	}()
	return o, errCh
}

func (o *observerWithReconnect) Sub(exchangeName string,
	reply interface{},
	exchangeCfg *ExchangeConfig,
	queueCfg *QueueConfig,
	queueBindCfg *QueueBindConfig,
	consumeCfg *ConsumeConfig,
) (<-chan Event, <-chan error, error) {

}

type chanErrorSenderWithCap struct {
	errChannel chan<- error

	cap        int
	currentCap int
}

func newChanErrorSenderWithCap(cap int, ch chan<- error) *chanErrorSenderWithCap {
	return &chanErrorSenderWithCap{
		cap:        cap,
		errChannel: ch,
	}
}

func (s *chanErrorSenderWithCap) Send(err error) {
	if s.currentCap < s.cap {
		s.currentCap++
		go func() {
			s.errChannel <- err
			s.currentCap--
		}()
	}
}

type sleeper struct {
	cap        int
	currentCap int
	baseDelay  time.Duration
}

func NewSleeper(cap int, delay time.Duration) *sleeper {
	return &sleeper{
		currentCap: 1,
		cap:        cap,
		baseDelay:  delay,
	}
}

func (s *sleeper) Sleep() {
	time.Sleep(s.baseDelay * time.Duration(s.currentCap))
}

func (s *sleeper) Increase(a int) {
	if s.currentCap < s.cap {
		s.currentCap += a
	}
}

func (s *sleeper) Drop() {
	s.currentCap = 1
}
