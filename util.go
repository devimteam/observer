package observer

import (
	"math/rand"
	"sync"
	"time"
)

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

func initSubscribeConfigs(exchangeCfg *ExchangeConfig,
	queueCfg *QueueConfig,
	queueBindCfg *QueueBindConfig,
	consumeCfg *ConsumeConfig,
) (
	*ExchangeConfig,
	*QueueConfig,
	*QueueBindConfig,
	*ConsumeConfig,
) {
	if exchangeCfg == nil {
		exchangeCfg = DefaultExchangeConfig()
	}
	if queueCfg == nil {
		queueCfg = DefaultQueueConfig()
	}
	if queueBindCfg == nil {
		queueBindCfg = DefaultQueueBindConfig()
	}
	if consumeCfg == nil {
		consumeCfg = DefaultConsumeConfig()
	}
	return exchangeCfg, queueCfg, queueBindCfg, consumeCfg
}

var randomSymbols = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func init() {
	rand.Seed(time.Now().Unix())
}

func genRandomString(length int) string {
	n := len(randomSymbols)
	b := make([]rune, length)
	for i := range b {
		b[i] = randomSymbols[rand.Intn(n)]
	}
	return string(b)
}

type MutexWithBool struct {
	mx       sync.Mutex
	isLocked bool
}

func (c *MutexWithBool) Lock() {
	c.mx.Lock()
	c.isLocked = true
}

func (c *MutexWithBool) Unlock() {
	c.isLocked = false
	c.mx.Unlock()
}

func (c *MutexWithBool) IsLocked() bool {
	return c.isLocked
}
