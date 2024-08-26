package internal

import "sync/atomic"

type LRU struct {
	buffers [MAXPOOLSIZE]slotInfo
}

func InitialLRU() LRU {
	var lru LRU
	for i := range lru.buffers {
		lru.buffers[i].num = -1
		lru.buffers[i].access = new(atomic.Int32)
	}
	return lru
}

func (l *LRU) addNum(num PageID) (int, bool) {
	for i := range l.buffers {
		if l.buffers[i].num == -1 {
			l.buffers[i].num = num
			for j := range l.buffers {
				l.buffers[j].access.Add(-1) //change to +1 for MRU
			}
			l.buffers[i].access.Store(1)
			return l.buffers[i].pos, true
		}
	}

	return -1, false
}

func (l *LRU) deleteNum(num PageID) int {
	for i := range l.buffers {
		if num == l.buffers[i].num {
			l.buffers[i].num = -1
			return l.buffers[i].pos
		}
	}
	return -1
}

func (l *LRU) freeNum(num PageID) int {
	for {
		for i := range l.buffers {
			if l.buffers[i].access.Load() <= 0 {
				l.buffers[i].num = num
				return l.buffers[i].pos
			}
			l.buffers[i].access.Add(-1)
		}
	}
}

func (l *LRU) findNum(num PageID) (int, bool) {
	for i := range l.buffers {
		if num == l.buffers[i].num {
			l.buffers[i].access.Add(1)
			return l.buffers[i].pos, true
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
