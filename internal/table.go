package internal

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
)

/*
TableName must be checked to be within 1 byte range
ColumnNames must have length within 1 byte range
Columns slice must preserve order
*/
type Table struct {
	Columns       []Column
	Name          string //max size is maxuint8
	lastRowId     int64
	rowEmptyBytes uint64        //dynamic at runtime
	lastPage      uint64        //dynamic at runtime
	indices       *indexManager //dynamic at runtime created or loaded
	tableLock     *sync.RWMutex //dynamic at runtime created
}

func (t *Table) toBytes() []byte {
	buf := make([]byte, 1, 100)
	buf[0] = uint8(len(t.Name))
	buf = append(buf, t.Name...)
	//rowid 8 bytes put into bytes buffer
	buf = binary.LittleEndian.AppendUint64(buf, uint64(t.lastRowId))

	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(t.Columns)))
	for i := 0; i < len(t.Columns); i++ {
		buf = append(buf, t.Columns[i].toBytes()...)
	}
	return buf
}

func (t *Table) fromBytes(buf []byte) int {
	tableNameSize := int(buf[0])
	t.Name = string(buf[1 : 1+tableNameSize])
	byteIndex := int(1 + tableNameSize)
	t.lastRowId = int64(binary.LittleEndian.Uint64(buf[byteIndex : byteIndex+8]))
	byteIndex += 8

	numCols := binary.LittleEndian.Uint16(buf[byteIndex:])
	byteIndex += 2
	t.Columns = make([]Column, numCols)
	for i := 0; i < int(numCols); i++ {
		newCol := Column{}
		offset := newCol.fromBytes(buf[byteIndex:])
		byteIndex += offset
		t.Columns[i] = newCol
	}
	return byteIndex
}

func (t *Table) GenerateRowBytes() uint64 {
	if t.rowEmptyBytes == 0 {
		bytelength := 0
		for _, val := range t.Columns {
			bytelength += int(val.columnSize)
		}
		t.rowEmptyBytes = uint64(bytelength)
	}
	return t.rowEmptyBytes
}

func (t *Table) GenerateFields() {
	offset := 0
	for i := range t.Columns {
		t.Columns[i].columnIndex = i
		t.Columns[i].columnOffset = offset
		offset += int(t.Columns[i].columnSize)
	}
}

func (t *Table) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Name: %s\n", t.Name))
	for _, col := range t.Columns {
		sb.WriteString(fmt.Sprintf("Column: %s\n", col.columnName))
		sb.WriteString(fmt.Sprintf("ColumnType: %d\n", col.columnType))
		sb.WriteString(fmt.Sprintf("ColumnSize: %d\n", col.columnSize))
	}
	return sb.String()
}
