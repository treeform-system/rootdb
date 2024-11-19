package internal

import "sync/atomic"

type lruSlot struct {
	page   PageID
	access *atomic.Int32
}

type LRU struct {
	buffers [MAXPOOLSIZE]lruSlot
}

func InitialLRU() LRU {
	var lru LRU
	for i := range lru.buffers {
		lru.buffers[i].page = -1
		lru.buffers[i].access = new(atomic.Int32)
	}
	return lru
}

func (l *LRU) addPage(page PageID) (int, bool) {
	for i := range l.buffers {
		if l.buffers[i].page == -1 {
			l.buffers[i].page = page
			for j := range l.buffers {
				l.buffers[j].access.Add(-1) //change to +1 for MRU
			}
			l.buffers[i].access.Store(1)
			return i, true
		}
	}

	return -1, false
}

func (l *LRU) deletePage(page PageID) int {
	for i := range l.buffers {
		if page == l.buffers[i].page {
			l.buffers[i].page = -1
			return i
		}
	}
	return -1
}

func (l *LRU) freePage(page PageID) int {
	for {
		for i := range l.buffers {
			if l.buffers[i].access.Load() <= 0 {
				l.buffers[i].page = page
				return i
			}
			l.buffers[i].access.Add(-1)
		}
	}
}

func (l *LRU) findPage(page PageID) (int, bool) {
	for i := range l.buffers {
		if page == l.buffers[i].page {
			l.buffers[i].access.Add(1)
			return i, true
		}
	}
	return -1, false
}

// func main() {
// 	mylru := InitialLRU()
// 	mylru.AddNum(9)
// 	mylru.AddNum(0)
// 	mylru.AddNum(1)
// 	mylru.AddNum(7)
// 	mylru.AddNum(6)
// 	fmt.Println(mylru.slots)
// 	mylru.AddNum(0)
// 	fmt.Println(mylru.slots)
// 	mylru.AddNum(8)
// 	fmt.Println(mylru.slots)
// }
