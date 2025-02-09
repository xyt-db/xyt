package xyt

import (
	"sync"
	"unsafe"

	"github.com/xyt-db/xyt/server"
)

// Stats contain the number of records, and the total size
// of those records (as best we can reckon this) for a given
// Dataset.
type Stats struct {
	locker      *sync.Mutex
	RecordCount uint32
	TotalSize   uint64
}

func newStats() *Stats {
	return &Stats{
		locker:      new(sync.Mutex),
		RecordCount: 0,
		TotalSize:   0,
	}
}

func (s *Stats) addRecord(r *server.Record) {
	s.locker.Lock()
	defer s.locker.Unlock()

	s.RecordCount++
	s.TotalSize += (uint64(unsafe.Sizeof(*r)) + uint64(unsafe.Sizeof(*r.Meta)))
}
