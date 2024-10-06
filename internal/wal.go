package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	COMPLETE_TRANSACTION int = iota
	START_TRANSACTION
	END_TRANSACTION
)

type walSingle struct {
	walFile          *os.File
	transactionCount int
	tableColumns     []Column
	heldCells        [][]Cell
	writerLock       *sync.Mutex
}

func InitializeWal(dir string, tablename string, cols []Column) error {
	fPath := filepath.Join(dir, fmt.Sprintf("%s_wal.db", tablename))
	newWal := new(walSingle)

	f, err := os.OpenFile(fPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	newWal.walFile = f
	newWal.tableColumns = cols
	newWal.writerLock = new(sync.Mutex)
	if fi.Size() == 0 {
		return nil
	}
	//wal file not empty
	readF, err := os.OpenFile(fPath, os.O_RDONLY, 0666)
	if err != nil {
		f.Close()
		return err
	}
	_ = readF
	return nil
}

func (w *walSingle) insertDataSingular(cells [][]Cell) error {
	return nil
}

func (w *walSingle) closeWal() error {
	return w.walFile.Close()
}

type transaction struct {
	transactionType int
	cells           [][]Cell //references will be in byte slices in same struct
	dataBytes       [][]byte //each row is one slice
}
