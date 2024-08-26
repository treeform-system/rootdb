package internal

import (
	"fmt"
	"strings"
)

type BitSet struct {
	bytes []byte
}

func InitializeBitSet(n uint64) BitSet {
	return BitSet{make([]byte, (n+8-1)/8)}
}

func (b *BitSet) fromBytes(x []byte) {
	b.bytes = x
}

func (b *BitSet) setBit(pos int) {
	bytepos := pos / 8
	if bytepos >= len(b.bytes) {
		return
	}
	smallPos := pos % 8
	b.bytes[bytepos] |= (1 << smallPos)
}

func (b *BitSet) clearBit(pos int) {
	bytepos := pos / 8
	if bytepos >= len(b.bytes) {
		return
	}
	smallPos := pos % 8
	mask := byte(^(1 << smallPos))
	b.bytes[bytepos] &= mask
}

func (b *BitSet) hasBit(pos int) bool {
	bytepos := pos / 8
	if bytepos >= len(b.bytes) {
		return false
	}
	smallPos := pos % 8
	val := b.bytes[bytepos] & (1 << smallPos)
	return (val > 0)
}

func (b *BitSet) ToString() string {
	return strings.Trim(fmt.Sprintf("%08b", b.bytes), "[]")
}

func (b *BitSet) Size() uint64 {
	return uint64(len(b.bytes))
}
