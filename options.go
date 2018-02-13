package observer

import "time"

type options struct {
	waitConnection bool
	timeoutBase    time.Duration
	timeoutCap     int
}

type Option func(*options)

func TimeoutOption(base time.Duration, cap int) Option {
	return func(options *options) {
		options.timeoutBase = base
		options.timeoutCap = cap
	}
}

func WaitForConnection(should bool) Option {
	return func(options *options) {
		options.waitConnection = should
	}
}
