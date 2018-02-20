package observer

import "time"

type options struct {
	waitConnection         bool
	waitConnectionDeadline time.Duration
	timeoutBase            time.Duration
	timeoutCap             int
	subErrorChanBuffer     int
	subEventChanBuffer     int
	pubUseOneChannel       bool
}

const (
	defaultWaitDeadline = time.Second * 5
	defaultEventBuffer  = 1
	defaultErrorBuffer  = 1
)

func defaultOptions() options {
	return options{
		waitConnectionDeadline: defaultWaitDeadline,
		subEventChanBuffer:     defaultEventBuffer,
		subErrorChanBuffer:     defaultErrorBuffer,
	}
}

type Option func(*options)

func TimeoutOption(base time.Duration, cap int) Option {
	return func(options *options) {
		options.timeoutBase = base
		options.timeoutCap = cap
	}
}

func WaitConnection(should bool) Option {
	return func(options *options) {
		options.waitConnection = should
	}
}

func WaitDeadline(deadline time.Duration) Option {
	return func(options *options) {
		options.waitConnectionDeadline = deadline
	}
}

func EventChanBuffer(a int) Option {
	return func(options *options) {
		options.subEventChanBuffer = a
	}
}

func ErrorChanBuffer(a int) Option {
	return func(options *options) {
		options.subErrorChanBuffer = a
	}
}
