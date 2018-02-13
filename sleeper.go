package observer

import "time"

type sleeper struct {
	cap        int
	currentCap uint
	baseDelay  time.Duration
}

func NewSleeper(cap int, delay time.Duration) *sleeper {
	return &sleeper{
		currentCap: 0,
		cap:        cap,
		baseDelay:  delay,
	}
}

func (s *sleeper) Sleep() {
	time.Sleep(s.baseDelay * time.Duration(expOf2(s.currentCap)))
}

func (s *sleeper) Inc() {
	if s.cap < 0 || s.currentCap < uint(s.cap) {
		s.currentCap++
	}
}

func (s *sleeper) Drop() {
	s.currentCap = 0
}

func (s *sleeper) Value() uint {
	return s.currentCap
}

// Returns 2^x
// If x is zero, returns 0
func expOf2(x uint) uint {
	if x == 0 {
		return 0
	}
	return 2 << uint(x-1)
}
