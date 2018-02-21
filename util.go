package observer

import (
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type logger struct {
	errChannel chan<- error

	cap        int
	currentCap int
	level      int
}

func newLogger(level int, cap int, ch chan<- error) *logger {
	return &logger{
		cap:        cap,
		errChannel: ch,
		level:      level,
	}
}

func (s *logger) Log(level int, err error) {
	if level < s.level {
		if s.currentCap < s.cap {
			s.currentCap++
			go func() {
				s.errChannel <- err
				s.currentCap--
			}()
		}
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

func NewSimpleMessage(codec Codec, data interface{}) (msg amqp.Publishing, err error) {
	msg.Body, err = codec.NewResponse(data).Body()
	if err != nil {
		return
	}
	msg.ContentType = codec.ContentType() // todo: should be fmt.Sprintf("%T", data)
	msg.Timestamp = time.Now()
	msg.Type = http.DetectContentType(msg.Body)
	return
}
