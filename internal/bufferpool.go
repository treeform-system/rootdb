package internal

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

/*
write for transactions and locking writes
*/

type PageID int64

type bufferPoolManager struct {
	dir      string
	allpools map[string]*bufferPool
}

func NewBufferPoolManager(dir string) *bufferPoolManager {
	newManager := bufferPoolManager{
		dir:      dir,
		allpools: make(map[string]*bufferPool),
	}

	return &newManager
}

func (bm *bufferPoolManager) newPool(tablename string, dir string, cols []Column) (int64, uint64) {
	newPool := &bufferPool{
		slots:   [MAXPOOLSIZE]*internalSlots{},
		mxread:  &sync.Mutex{},
		mxwrite: &sync.Mutex{},
		pagemx:  &sync.RWMutex{},
		lru:     InitialLRU(),
		columns: cols,
	}
	for i := range newPool.lru.buffers {
		newPool.lru.buffers[i].pos = i
	}
	for i := range newPool.slots {
		newPool.slots[i] = new(internalSlots)
	}

	filePathStr := filepath.Join(dir, fmt.Sprintf("%s.db", tablename))
	_, err := os.Stat(filePathStr)
	if err != nil {
		f, err := os.Create(filePathStr)
		if err != nil {
			panic(err)
		}
		f.Close()
	}
	newPool.tablefileRead, _ = os.OpenFile(filePathStr, os.O_RDONLY, 0644)
	newPool.tablefileWrite, _ = os.OpenFile(filePathStr, os.O_WRONLY, 0644)
	bm.allpools[tablename] = newPool

	fi, err := newPool.tablefileRead.Stat()
	if err != nil {
		panic(err)
	}
	if fi.Size() == 0 {
		return 1, 0
	}
	lastPage := fi.Size()/PAGESIZE - 1
	allrows := newPool.FetchPage(PageID(lastPage))
	max := int64(1)
	primaryIndex := 0
	for i := range newPool.columns {
		if newPool.columns[i].columnIsPrimary {
			primaryIndex = i
			break
		}
	}
	for i := range allrows {
		if allrows[i][primaryIndex].AsInt() > int64(max) {
			max = allrows[i][primaryIndex].AsInt()
		}
	}
	return max, uint64(lastPage)
}

// return last page modified
func (bm *bufferPoolManager) InsertData(tablename string, pageid PageID, data [][]Cell, indexData *[][2]int64) (PageID, error) {
	pool, ok := bm.allpools[tablename]
	if !ok {
		return 0, fmt.Errorf("table name: \"%s\" does not exist", tablename)
	}

	pageToModify, err := pool.rawFetchPage(pageid)
	if err != nil {
		return 0, errors.Join(errors.New("internal error fetching page: "), err)
	}

	rows := make([][]byte, len(data))
	rowsize := 0
	for i := range pool.columns {
		rowsize += int(pool.columns[i].columnSize)
	}
	for i := range data {
		nullColumns := InitializeBitSet(uint64(len(data[i]) + 1))
		nullColumns.setBit(len(pool.columns) + 1)
		newrow := make([]byte, nullColumns.Size()+uint64(rowsize))
		offset := int(nullColumns.Size())
		for j := range data[i] {
			colsize := int(pool.columns[j].columnSize)
			if data[i][j] == nil {
				offset += colsize
				continue
			}
			nullColumns.setBit(j)
			copy(newrow[offset:offset+colsize], data[i][j])
			offset += colsize
		}
		copy(newrow[0:nullColumns.Size()], nullColumns.bytes)
		rows[i] = newrow
	}

	pool.mxwrite.Lock()
	defer pool.mxwrite.Unlock()
	f := pool.tablefileWrite

	pages := make([][PAGESIZE]byte, 1, 10)
	pages[0] = pageToModify //copies by value
	pgIndex := 0

	//numberOfPage := binary.LittleEndian.Uint64(buf[0:8])
	rowNums := binary.LittleEndian.Uint16(pages[pgIndex][8:10])
	//checksum := buf[10:26]

	offset := 26 + int(rowNums)*len(data[0])

	pgNum := pageid
	for i := range rows {
		if offset+len(rows[i]) > PAGESIZE {
			//create new page and write old page
			//binary.LittleEndian.PutUint64(pages[pgIndex][0:8], uint64(pgNum))
			binary.LittleEndian.PutUint16(pages[pgIndex][8:10], rowNums)
			checksum := md5.Sum(pages[pgIndex][26:])
			copy(pages[pgIndex][10:26], checksum[:])

			rowNums = 0
			pgNum += 1
			buf := [PAGESIZE]byte{}
			binary.LittleEndian.PutUint64(buf[:], uint64(pgNum))
			offset = 26
			pages = append(pages, buf)
			pgIndex += 1
		}
		copy(pages[pgIndex][offset:], rows[i])
		(*indexData)[i][1] = int64(offset) + int64(pgNum*PAGESIZE)
		offset += len(rows[i])
		rowNums += 1
	}

	checksum := md5.Sum(pages[pgIndex][26:])
	copy(pages[pgIndex][10:26], checksum[:])
	binary.LittleEndian.PutUint16(pages[pgIndex][8:10], rowNums)

	f.Seek(int64(pageid)*PAGESIZE, 0)
	for i := range pages {
		_, err := f.Write(pages[i][:])
		if err != nil {
			return 0, err
		}
	}
	f.Sync()

	//deletes page from bufferpool and all data should be written to disk by this point
	pool.pagemx.Lock()
	pool.deletePage(pageid)
	pool.pagemx.Unlock()

	return pgNum, nil
}

func (bm *bufferPoolManager) SelectDataRange(tablename string, start, end PageID) [][]Cell {
	allpages := make([][]Cell, 0, 100)

	pool := bm.allpools[tablename]
	for i := start; i <= end; i++ {
		rows := pool.FetchPage(i)

		allpages = append(allpages, rows...)
	}
	return allpages
}

func (bm *bufferPoolManager) SelectDataPages(tablename string, nums []PageID) [][]Cell {
	allpages := make([][]Cell, 0, 100)

	pool := bm.allpools[tablename]
	for i := range nums {
		rows := pool.FetchPage(nums[i])
		allpages = append(allpages, rows...)
	}
	return allpages
}

func (bm *bufferPoolManager) close() {
	for _, val := range bm.allpools {
		val.tablefileRead.Close()
		val.tablefileWrite.Close()
	}
}

type bufferPool struct {
	slots   [MAXPOOLSIZE]*internalSlots
	mxread  *sync.Mutex
	mxwrite *sync.Mutex
	pagemx  *sync.RWMutex

	tablefileRead  *os.File
	tablefileWrite *os.File
	lru            LRU
	columns        []Column
}

// returns copy of cell rows
func (b *bufferPool) FetchPage(pageid PageID) [][]Cell {
	b.pagemx.RLock()
	pagepos, ok := b.lru.findNum(pageid)
	if ok {
		tmprows := b.slots[pagepos].returnClone()
		b.pagemx.RUnlock()
		return tmprows
	}
	//getslotindex and allocate page from buffer if none free free LRU slot
	b.pagemx.RUnlock()
	b.pagemx.Lock()
	defer b.pagemx.Unlock()
	pos, ok := b.lru.addNum(pageid)
	if !ok {
		pos = b.lru.freeNum(pageid)
		b.slots[pos] = nil //frees' the slice in that slot
	}
	err := b.AllocatePage(pageid, pos)
	if err != nil {
		return nil
	}
	return b.slots[pos].returnClone()
}

func (b *bufferPool) rawFetchPage(pageid PageID) ([PAGESIZE]byte, error) {
	b.pagemx.RLock()
	pagepos, ok := b.lru.findNum(pageid)
	if ok {
		b.pagemx.RUnlock()
		return (b.slots[pagepos].buf), nil
	}
	//getslotindex and allocate page from buffer if none free free LRU slot
	b.pagemx.RUnlock()
	b.pagemx.Lock()
	defer b.pagemx.Unlock()
	pos, ok := b.lru.addNum(pageid)
	if !ok {
		pos = b.lru.freeNum(pageid)
		b.slots[pos] = nil //frees' the slice in that slot
	}
	err := b.AllocatePage(pageid, pos)
	if err != nil {
		return [PAGESIZE]byte{}, errors.New("error fetching raw bytes")
	}
	return (b.slots[pos].buf), nil
}

// read page from disk
func (b *bufferPool) AllocatePage(pageid PageID, pos int) error {
	b.mxread.Lock()
	defer b.mxread.Unlock()

	_, err := b.tablefileRead.Seek(int64(pageid)*PAGESIZE, 0)
	if err != nil {
		return err
	}
	_, err = b.tablefileRead.Read(b.slots[pos].buf[:])
	if err != nil {
		return err
	}
	buf := b.slots[pos].buf[:] //pointer to underlying array without copy

	rowNums := binary.LittleEndian.Uint16(buf[8:10])
	checksum := buf[10:26]
	checksumcheck := md5.Sum(buf[26:])
	//fmt.Println(checksum, checksumcheck)
	if !bytes.Equal(checksum, checksumcheck[:]) {
		return fmt.Errorf("page %d has been corrupted", pageid)
	}
	rows := make([][]Cell, 0, rowNums)
	offset := 26
	numrows := 0
	rowsize := 0
	for i := range b.columns {
		rowsize += int(b.columns[i].columnSize)
	}
	bitset := InitializeBitSet(uint64(len(b.columns) + 1))
	bitsetsize := bitset.Size()

	for ; offset < (PAGESIZE)-int(uint64(rowsize)+(bitsetsize)); offset += (int(rowsize) + int(bitsetsize)) {
		row := make([]Cell, len(b.columns))
		tmprow := buf[offset : offset+int(rowsize)+int(bitsetsize)]
		var rowbitset BitSet
		rowbitset.fromBytes(tmprow[:bitsetsize])
		if !rowbitset.hasBit(len(b.columns) + 1) {
			continue
		}

		for k, col := range b.columns {
			if !rowbitset.hasBit(col.columnIndex) {
				row[k] = nil
				continue
			}
			celloffset := int(bitsetsize) + col.columnOffset
			row[k] = Cell(tmprow[celloffset : celloffset+int(col.columnSize) : celloffset+int(col.columnSize)])
		}
		rows = append(rows, row)

		numrows++
		if numrows >= int(rowNums) {
			break
		}
	}

	// for i := range rows {
	// 	for j := range rows[i] {
	// 		switch b.columns[j].columnType {
	// 		case INT:
	// 			fmt.Printf("(%d) ", rows[i][j].AsInt())
	// 		case FLOAT:
	// 			fmt.Printf("(%f) ", rows[i][j].AsFloat())
	// 		case BOOL:
	// 			fmt.Printf("(%t) ", rows[i][j].AsBool())
	// 		case CHAR:
	// 			fmt.Printf("(%s) ", rows[i][j].AsString())
	// 		}
	// 	}
	// 	fmt.Println()
	// }

	b.slots[pos].rows = rows
	return nil
}

func (b *bufferPool) deletePage(num PageID) {
	pos := b.lru.deleteNum(num)
	if pos == -1 {
		return
	}
	b.slots[pos].rows = nil
}

type internalSlots struct {
	rows [][]Cell
	buf  [PAGESIZE]byte
}

func (is *internalSlots) returnClone() [][]Cell {
	cloneRows := make([][]Cell, len(is.rows))
	for i := range is.rows {
		row := make([]Cell, len(is.rows[i]))
		for k := range row {
			cloneCell := make(Cell, len(is.rows[i][k]))
			copy(cloneCell, is.rows[i][k])
			row[k] = cloneCell
		}
		cloneRows[i] = row
	}
	return cloneRows
}

type slotInfo struct {
	num PageID
	// isDirty bool //write code to write dirty
	pos    int
	access *atomic.Int32
}
