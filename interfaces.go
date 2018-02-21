package observer

type Observer interface {
	Sub(service string,
		reply interface{},
		config *ExchangeConfig,
		queueConfig *QueueConfig,
		bindConfig *QueueBindConfig,
		consumeConfig *ConsumeConfig,
	) (eventChan <-chan Event, errorChan <-chan error, doneChan chan<- bool)
	Pub(service string,
		data interface{},
		config *ExchangeConfig,
		publishConfig *PublishConfig,
	) error
}

// Codec is an interface that encodes message on pub and decodes it on sub.
type Codec interface {
	Encoder
	Decoder
}

type Encoder interface {
	Encode(interface{}) ([]byte, error)
}

type Decoder interface {
	Decode([]byte, interface{}) error
}
