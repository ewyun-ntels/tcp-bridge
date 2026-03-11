package tid

import (
	"sync/atomic"
	"time"
)

var (
	globalCounter uint32
	lastResetDate int32
)

// Next returns a sequential transaction ID with a daily reset.
func Next() uint32 {
	now := time.Now()
	currentDate := int32(now.Year()*10000 + int(now.Month())*100 + now.Day())

	lastDate := atomic.LoadInt32(&lastResetDate)
	if currentDate != lastDate {
		if atomic.CompareAndSwapInt32(&lastResetDate, lastDate, currentDate) {
			atomic.StoreUint32(&globalCounter, 0)
		}
	}

	tid := atomic.AddUint32(&globalCounter, 1)
	if tid == 0 {
		atomic.StoreUint32(&globalCounter, 1)
		return 1
	}

	return tid
}
