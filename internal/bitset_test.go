package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitSet(t *testing.T) {
	myset := InitializeBitSet(87)
	myset.setBit(4)
	require.Equal(t, "00010000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000", myset.ToString())

	myset.setBit(12)
	myset.setBit(27)
	myset.setBit(48)
	myset.setBit(50)
	myset.setBit(64)
	myset.setBit(0)
	myset.clearBit(27)
	myset.setBit(87)
	myset.setBit(86)

	require.Equal(t, myset.hasBit(4), true)
	require.Equal(t, myset.hasBit(9), false)
	require.Equal(t, myset.hasBit(12), true)
	require.Equal(t, myset.hasBit(27), false)
	require.Equal(t, myset.hasBit(48), true)
	require.Equal(t, myset.hasBit(50), true)
	require.Equal(t, myset.hasBit(64), true)
	require.Equal(t, myset.hasBit(0), true)
	require.Equal(t, myset.hasBit(87), true)
}
