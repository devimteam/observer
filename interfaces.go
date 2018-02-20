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

// Codec creates a CodecRequest to process each request.
type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode(interface{}) error
	NewRequest(req Request) CodecRequest
	NewResponse(data interface{}) CodecResponse
	ContentType() string
}

// CodecRequest decodes a request and encodes a response using a specific
// serialization scheme.
type CodecRequest interface {
	// Reads the request filling the RPC method args.
	ReadRequest(i interface{}) error
}

type CodecResponse interface {
	Body() ([]byte, error)
}
