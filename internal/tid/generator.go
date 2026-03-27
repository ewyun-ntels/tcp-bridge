package tid

import (
	"sync"
	"sync/atomic"
	"time"
)

var (
	globalCounter uint32
	lastResetDate int32
	resetMu       sync.Mutex
)

// Next returns a sequential transaction ID with a daily reset.
func Next() uint32 {
	now := time.Now()
	currentDate := int32(now.Year()*10000 + int(now.Month())*100 + now.Day())

	if atomic.LoadInt32(&lastResetDate) != currentDate {
		resetMu.Lock()
		if atomic.LoadInt32(&lastResetDate) != currentDate {
			atomic.StoreUint32(&globalCounter, 0)
			atomic.StoreInt32(&lastResetDate, currentDate)
		}
		resetMu.Unlock()
	}

	tid := atomic.AddUint32(&globalCounter, 1)
	if tid == 0 {
		atomic.StoreUint32(&globalCounter, 1)
		return 1
	}

	return tid
}
