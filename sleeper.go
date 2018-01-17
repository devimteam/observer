package observer

import "time"

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
	time.Sleep(s.baseDelay * time.Duration(expOf2(uint(s.currentCap))))
}

func (s *sleeper) Inc() {
	if s.currentCap < s.cap || s.cap < 0 {
		s.currentCap++
	}
}

func (s *sleeper) Drop() {
	s.currentCap = 1
}

func expOf2(x uint) uint {
	return 2 << x
}
