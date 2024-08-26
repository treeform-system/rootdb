package internal

import (
	"encoding/binary"
	"math"
)

/*
Contain functions to convert byte slice to appropriate datatype as supported by database
*/
type Cell []byte

func (c *Cell) AsInt() int64 {
	return int64(binary.LittleEndian.Uint64(*c))
}

func (c *Cell) AsFloat() float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(*c))
}

func (c *Cell) AsBool() bool {
	return (*c)[0] != 0
}

func (c *Cell) AsString() string {
	return string(*c)
}
