package internal

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeletePage(t *testing.T) {
	panic("not implemented")
}

func TestFindPage(t *testing.T) {
	panic("not implemented")
}

func TestFreePage(t *testing.T) {
	panic("not implemented")
}

func TestClock(t *testing.T) {
	clock := InitialClock()

	for i := 0; i < MAXPOOLSIZE; i++ {
		pos, ok := clock.addPage(PageID(int64(i)))
		require.True(t, ok)
		require.Equal(t, i, pos)
	}

	for i := 0; i < MAXPOOLSIZE; i++ {
		pos, ok := clock.findPage(PageID(int64(i)))
		require.True(t, ok)
		require.Equal(t, i, pos)
	}

	idx, added := clock.addPage(PageID(int64(MAXPOOLSIZE)))
	require.False(t, added)
	require.Equal(t, idx, -1)

	pos := clock.deletePage(PageID(int64(MAXPOOLSIZE)))
	require.Equal(t, pos, -1)

	var test_lock sync.RWMutex
	test_locked := func(f func()) {
		test_lock.Lock()
		f()
		test_lock.Unlock()
	}

	concurrencyTest := func(clock *Clock) {
		test_locked(func() {
			pos, ok := clock.findPage(PageID(int64(0)))
			if !ok {
				require.Equal(t, pos, -1) // invariant: !ok <=> pos == -1

				pos = clock.freePage(PageID(int64(0)))
				require.GreaterOrEqual(t, pos, 0)
				require.LessOrEqual(t, pos, MAXPOOLSIZE)
			}

			require.Equal(t, pos, clock.deletePage(PageID(int64(0))))

			pos, ok = clock.addPage(PageID(int64(0)))
			require.GreaterOrEqual(t, pos, 0)
			require.True(t, ok)
		})

		test_locked(func() {
			for i := 0; i < MAXPOOLSIZE; i++ {
				pos = clock.freePage(PageID(int64(i)))

				require.GreaterOrEqual(t, pos, 0)
				require.LessOrEqual(t, pos, MAXPOOLSIZE)

				if pos != i {
					require.Equal(t, clock.deletePage(PageID(int64(i))), -1)
					idx, found := clock.findPage(PageID(int64(i)))
					require.Equal(t, idx, -1)
					require.False(t, found)
				}
			}
		})
	}

	for i := 0; i < 100; i++ {
		go concurrencyTest(&clock)
	}

}
