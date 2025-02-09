package xyt

import (
	"sync"
	"unsafe"

	"github.com/xyt-db/xyt/server"
)

type Stats struct {
	locker      *sync.Mutex
	RecordCount uint32
	TotalSize   uint64
}

func NewStats() *Stats {
	return &Stats{
		locker:      new(sync.Mutex),
		RecordCount: 0,
		TotalSize:   0,
	}
}

func (s *Stats) AddRecord(r *server.Record) {
	s.locker.Lock()
	defer s.locker.Unlock()

	s.RecordCount++
	s.TotalSize += uint64(unsafe.Sizeof(*r))
}
