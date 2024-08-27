package internal

import (
	"crypto/md5"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
)

//write indexing connection and fix data types for uint

type Backend struct {
	dir        string
	tables     []Table
	bufferPool *bufferPoolManager
}

func CreateNewDatabase(dir string) *Backend {
	buf := make([]byte, 100) //reserves first hundred bytes of main file for header
	headername := []byte("RootDB MAINFILE\x00")
	copy(buf[0:16], headername)
	binary.LittleEndian.PutUint16(buf[16:18], uint16(PAGESIZE))

	f, err := os.Create(filepath.Join(dir, "main.db"))
	if err != nil {
		panic("unexpected error creating new main database file")
	}
	defer f.Close()
	_, err = f.Write(buf)
	if err != nil {
		panic(err)
	}
	return &Backend{dir: dir, tables: make([]Table, 0), bufferPool: NewBufferPoolManager(dir)}
}

func OpenExistingDatabase(dir string) (*Backend, error) {
	b := Backend{dir: dir, bufferPool: NewBufferPoolManager(dir)}

	allContent, err := os.ReadFile(filepath.Join(dir, "main.db"))
	if os.IsNotExist(err) {
		return nil, errors.New("database directory was tampered with, main file gone")
	} else if err != nil {
		return nil, err
	}

	numTables := binary.LittleEndian.Uint16(allContent[16:18])
	b.tables = make([]Table, numTables+5)
	if numTables == 0 {
		return &b, nil
	}

	offset := 100
	for i := 0; i < int(numTables); i++ {
		newtable := Table{}
		lenTable := newtable.fromBytes(allContent[offset:])
		newtable.GenerateFields()
		offset += lenTable
		b.tables[i] = newtable
	}

	for i, tab := range b.tables {
		lr, lp := b.bufferPool.newPool(tab.Name, b.dir, b.tables[i].Columns)
		b.tables[i].lastPage = lp
		b.tables[i].lastRowId = lr
	}

	return &b, nil
}

func (b *Backend) CreateTable(q Query) error {
	_, exists := b.checkTableExist(q)
	if exists {
		return errors.New("Table already exist")
	}
	newtable := Table{lastRowId: 0} //lowest number for primary key must be 1 and nonzero
	if !(len(q.TableName) > 0 && len(q.TableName) < 255) {
		return errors.New("table name too large in size")
	}
	newtable.Name = q.TableName
	newtable.Columns = make([]Column, len(q.Fields))
	newtable.tableLock = new(sync.RWMutex)

	if len(q.TableConstruction.primary) != 1 {
		return errors.New("currently only exactly one primary key supported")
	}
	var primaryName string
	for _, field := range q.TableConstruction.fieldsWTypes {
		if field[0] == q.TableConstruction.primary[0] {
			primaryName = field[0]
			if field[1] != "INT" {
				return errors.New("primary key must be Integer at this moment")
			}
		}
	}

	totalRowSize := 0
	for i, construct := range q.TableConstruction.fieldsWTypes {
		newColumn := Column{}
		if !(len(construct[0]) > 0 && len(construct[0]) < 255) {
			return errors.New("table name too large in size")
		}
		newColumn.columnName = construct[0]
		isUnique := slices.Contains(q.TableConstruction.unique, newColumn.columnName)
		isNotNullable := slices.Contains(q.TableConstruction.notnullable, newColumn.columnName)
		isNullable := slices.Contains(q.TableConstruction.nullable, newColumn.columnName)

		if isNullable && isNotNullable {
			return errors.New("column cannot be both nullable and non-nullable")
		}
		newColumn.columnIsUnique = isUnique
		newColumn.columnIsNullable = !isNotNullable
		switch construct[1] { //Uses reserved types list in parser.go
		case "INT":
			newColumn.columnType = INT
			newColumn.columnSize = 8 //(bytes)
			if slices.Contains(q.TableConstruction.primary, newColumn.columnName) {
				newColumn.columnIsPrimary = true
				newColumn.columnIsUnique = true
				newColumn.columnIsNullable = false
			}
		case "FLOAT":
			newColumn.columnType = FLOAT
			newColumn.columnSize = 8 //(bytes)
		case "BOOL":
			newColumn.columnType = BOOL
			newColumn.columnSize = 1 //(bytes)
		case "CHAR":
			newColumn.columnType = CHAR
			if len(construct) != 3 {
				return errors.New("size needed for char field in table")
			}
			fieldSize, err := strconv.ParseInt(construct[2], 10, 64)
			if err != nil {
				return errors.Join(errors.New("error in table construction of size of CHAR field: "), err)
			}
			if !(fieldSize >= 1 && fieldSize <= 255) {
				return errors.New("size for char field must be between 1 and 255")
			}
			newColumn.columnSize = uint8(fieldSize)
		default:
			return errors.ErrUnsupported
		}
		totalRowSize += int(newColumn.columnSize)
		newtable.Columns[i] = newColumn
	}

	if totalRowSize >= 4070 {
		return errors.New("row size for this table exceeds max row size")
	}

	f, err := os.Create(filepath.Join(b.dir, fmt.Sprintf("%s.db", newtable.Name)))
	if err != nil {
		return err
	}
	defer f.Close()

	buf := [PAGESIZE]byte{}                     //should remove once index values are pages
	binary.LittleEndian.PutUint64(buf[0:8], 0)  //pagenum
	binary.LittleEndian.PutUint16(buf[8:10], 0) //rownums
	checksum := md5.Sum(buf[26:])
	copy(buf[10:26], checksum[:])
	_, err = f.Write(buf[:])
	if err != nil {
		return err
	}

	newtable.lastPage = 0
	newtable.GenerateFields()
	newtable.indices = newIndexManager()
	err = newtable.indices.addIndex(b.dir, newtable.Name, primaryName)
	if err != nil {
		f.Close()
		os.Remove(filepath.Join(b.dir, fmt.Sprintf("%s.db", newtable.Name)))
		return err
	}
	b.tables = append(b.tables, newtable)
	b.writeTablesToDisk()
	b.bufferPool.newPool(newtable.Name, b.dir, newtable.Columns)
	return nil
}

func (b *Backend) writeTablesToDisk() {
	buf := make([]byte, 0, PAGESIZE)

	for _, table := range b.tables {
		buf = append(buf, table.toBytes()...)
	}

	oldfile := filepath.Join(b.dir, "main.db")
	newfile := filepath.Join(b.dir, "temp-main.db")

	f, err := os.OpenFile(oldfile, os.O_RDONLY, 0700)
	if err != nil {
		panic(err)
	}
	metabuf := make([]byte, 100)
	_, err = f.Read(metabuf)
	if err != nil {
		panic(err)
	}
	f.Close()
	newF, err := os.Create(newfile)
	if err != nil {
		panic(err)
	}
	binary.LittleEndian.PutUint16(metabuf[16:], uint16(len(b.tables)))
	_, err = newF.Write(metabuf)
	if err != nil {
		panic(err)
	}
	_, err = newF.Write(buf)
	if err != nil {
		panic(err)
	}
	newF.Close()
	os.Rename(newfile, oldfile)
}

func (b *Backend) Insert(q Query) error {
	tableToInsert, ok := b.checkTableExist(q)
	if !ok {
		return errors.New("Table does not exist")
	}

	allrows := make([][]Cell, 0, len(q.Inserts))
	insertColumns := make([]InsertColumn, len(tableToInsert.Columns)) //same length as table columns
	queryCols := make([]string, len(q.Fields))
	copy(queryCols, q.Fields)

	for i, col := range tableToInsert.Columns {
		insertColumns[i].columnSize = col.columnSize
		insertColumns[i].dataType = col.columnType
		isNull := true
		for j := range queryCols {
			if queryCols[j] == col.columnName {
				isNull = false
				for k := range q.Fields {
					if q.Fields[k] == col.columnName {
						insertColumns[i].insertIndex = k
						break
					}
				}
				queryCols = removeColFieldGen[string](queryCols, j)
				break
			}
		}
		if isNull && col.columnIsPrimary { //supposes primary key is rowid
			insertColumns[i].colType = COL_I_PRIMARYNULL
		} else if !isNull && col.columnIsPrimary {
			insertColumns[i].colType = COL_I_PRIMARYVALUED
		} else if isNull {
			if !col.columnIsNullable {
				return fmt.Errorf("column %s may not be null", col.columnName)
			}
			insertColumns[i].colType = COL_I_NULL
		} else if !isNull {
			insertColumns[i].colType = COL_I_VALUED
		}
	}
	if len(queryCols) > 0 {
		return fmt.Errorf("Columns may not exist: %s", strings.Join(queryCols, " - "))
	}

	tableToInsert.tableLock.Lock()
	defer tableToInsert.tableLock.Unlock()

	indexInserts := make([][2]int64, 0, len(q.Inserts))
	pageid := PageID(tableToInsert.lastPage) // must lock table before accessing these variables
	lastrownum := tableToInsert.lastRowId    // must lock table before accessing

	for _, val := range q.Inserts {
		cellRow := make([]Cell, len(insertColumns))

		for j := range insertColumns {
			if insertColumns[j].colType == COL_I_PRIMARYVALUED {
				num, err := strconv.ParseInt(val[insertColumns[j].insertIndex], 10, 64)
				if err != nil {
					return errors.Join(errors.New("Insert Query failed: "), err)
				}
				if num <= tableToInsert.lastRowId {
					return errors.New("Insert Query failed: non valid primary key provided")
				}
				lastrownum = num
				newIndexTuple := [2]int64{lastrownum, 0}
				indexInserts = append(indexInserts, newIndexTuple)
				cellRow[j] = make(Cell, insertColumns[j].columnSize)
				binary.LittleEndian.PutUint64(cellRow[j], uint64(num))
			} else if insertColumns[j].colType == COL_I_PRIMARYNULL {
				lastrownum++ //should always be valid
				cellRow[j] = make(Cell, insertColumns[j].columnSize)
				binary.LittleEndian.PutUint64(cellRow[j], uint64(lastrownum))
				newIndexTuple := [2]int64{lastrownum, 0}
				indexInserts = append(indexInserts, newIndexTuple)
			} else if insertColumns[j].colType == COL_I_VALUED {
				cellRow[j] = make(Cell, insertColumns[j].columnSize)
				switch insertColumns[j].dataType {
				case INT:
					n, err := strconv.ParseInt(val[insertColumns[j].insertIndex], 10, 64)
					if err != nil {
						return errors.Join(errors.New("Insert Query failed: "), err)
					}
					binary.LittleEndian.PutUint64(cellRow[j], uint64(n))
				case FLOAT:
					n, err := strconv.ParseFloat(val[insertColumns[j].insertIndex], 64)
					if err != nil {
						return errors.Join(errors.New("Insert Query failed: "), err)
					}
					binary.LittleEndian.PutUint64(cellRow[j], math.Float64bits(n))
				case BOOL:
					n, err := strconv.ParseBool(val[insertColumns[j].insertIndex])
					if err != nil {
						return errors.Join(errors.New("Insert Query failed: "), err)
					}
					if n {
						cellRow[j] = Cell([]byte{1})
					} else {
						cellRow[j] = Cell([]byte{0})
					}
				case CHAR:
					n := []byte(val[insertColumns[j].insertIndex])
					if len(n) > int(insertColumns[j].columnSize) {
						return errors.Join(errors.New("Insert Query failed: "), errors.New("string to insert larger than allowed"))
					}
					cellRow[j] = Cell(n)
				}
			} else if insertColumns[j].colType == COL_I_NULL {
				cellRow[j] = nil
			}
		}
		allrows = append(allrows, cellRow)
	}

	n, err := b.bufferPool.InsertData(tableToInsert.Name, pageid, allrows, &indexInserts)
	if err != nil {
		return err
	}
	for i := range indexInserts {
		// fmt.Println(indexInserts[i])
		err := tableToInsert.indices.primaryTree.insertNode(indexInserts[i][0], indexInserts[i][1])
		if err != nil {
			return err
		}
	}

	tableToInsert.lastPage = uint64(n)
	tableToInsert.lastRowId = int64(lastrownum)
	return nil
}

func (b *Backend) Select(q Query) (driver.Rows, error) {
	tmpTable, ok := b.checkTableExist(q)
	if !ok {
		return nil, errors.New("Table does not exist")
	}
	tmpTable.tableLock.RLock()
	defer tmpTable.tableLock.RUnlock()

	rows := &Rows{index: 0}
	rows.rows = make([][]Cell, 0)

	if q.Fields[0] == "*" {
		rows.columns = make([]ResultColumn, len(tmpTable.Columns))
		for i := range tmpTable.Columns {
			rows.columns[i] = ResultColumn{Name: tmpTable.Columns[i].columnName, ColumnType: tmpTable.Columns[i].columnType, columnPos: i}
		}
	} else {
		rows.columns = make([]ResultColumn, 0, len(q.Fields))
		for i := range tmpTable.Columns {
			for j := range q.Fields {
				if tmpTable.Columns[i].columnName == q.Fields[j] {
					rows.columns = append(rows.columns, ResultColumn{Name: tmpTable.Columns[i].columnName, ColumnType: tmpTable.Columns[i].columnType, columnPos: i})
					q.Fields = removeColFieldGen[string](q.Fields, j)
					break
				}
			}
		}
		if len(q.Fields) != 0 {
			return nil, fmt.Errorf("Columns not in table: %s", strings.Join(q.Fields, "|"))
		}
	}

	if len(q.Conditions) == 0 { //all pages queried in PageID order
		startPage := PageID(0)
		endPage := PageID(tmpTable.lastPage)
		allrows := b.bufferPool.SelectDataRange(tmpTable.Name, startPage, endPage)
		rows.rows = allrows
	} else {
		tmpConditions := make([]struct {
			Condition
			operand1Index int
			operand2Index int
		}, len(q.Conditions))
		for i := range q.Conditions { //check
			tmpConditions[i].Condition = q.Conditions[i]
			for j := range tmpTable.Columns {
				if q.Conditions[i].Operand1 == tmpTable.Columns[j].columnName {
					tmpConditions[i].operand1Index = j
				}
				if q.Conditions[i].Operand2IsField && q.Conditions[i].Operand2 == tmpTable.Columns[j].columnName {
					tmpConditions[i].operand2Index = j
				}
			}
			cond := tmpConditions[i]
			if cond.Operand2IsField && tmpTable.Columns[cond.operand1Index].columnType != tmpTable.Columns[cond.operand2Index].columnType {
				return nil, errors.New("cannot compare columns of different type")
			}

			if (tmpTable.Columns[cond.operand1Index].columnType == BOOL) && (cond.Operator == Gt || cond.Operator == Gte || cond.Operator == Lt || cond.Operator == Lte) {
				return nil, errors.New("cannot use this operator for comparing booleans")
			}

			if !cond.Operand2IsField {
				switch tmpTable.Columns[cond.operand1Index].columnType {
				case INT:
					_, err := strconv.ParseInt(cond.Operand2, 10, 64)
					if err != nil {
						return nil, err
					}
				case FLOAT:
					_, err := strconv.ParseFloat(cond.Operand2, 64)
					if err != nil {
						return nil, err
					}
				case BOOL:
					_, err := strconv.ParseBool(cond.Operand2)
					if err != nil {
						return nil, err
					}
				case CHAR:
				}
			}
		}

		tmprows := make([][]Cell, 0)
		var hasIndex bool = false

		for i := range tmpConditions { //checking for any indexes first
			cond := tmpConditions[i]
			if cond.Operand1 == tmpTable.indices.columnName {
				if cond.Operand2IsField {
					return nil, errors.New("cannot compare primary key to non-integer for now: ")
				}
				num, err := strconv.ParseInt(cond.Operand2, 10, 32)
				if err != nil {
					return nil, errors.New("primary key not an integer: ")
				}
				switch cond.Operator {
				case Eq:
					val, ok := tmpTable.indices.primaryTree.findKeyValue(num)
					if ok {
						var tmpPageID uint64 = uint64(val / PAGESIZE)
						cellRows := b.bufferPool.SelectDataPages(tmpTable.Name, []PageID{PageID(tmpPageID)})
						for i := range cellRows {
							if cellRows[i][cond.operand1Index].AsInt() == num {
								tmprows = append(tmprows, cellRows[i])
								break
							}
						}
					}
				case Gt, Gte:
					potentialLeaf := tmpTable.indices.primaryTree.findLeaf(tmpTable.indices.primaryTree.root, num)
					pageSets := make(map[int64]struct{})
					values := make(map[int64]struct{})
					for potentialLeaf != nil {
						for i := range potentialLeaf.keys {
							if num == potentialLeaf.keys[i] && cond.Operator == Gte {
								pageSets[potentialLeaf.values[i]/PAGESIZE] = struct{}{}
								values[potentialLeaf.keys[i]] = struct{}{}
							} else if potentialLeaf.keys[i] > num {
								pageSets[potentialLeaf.values[i]/PAGESIZE] = struct{}{}
								values[potentialLeaf.keys[i]] = struct{}{}
							}
						}
						potentialLeaf = potentialLeaf.next
					}
					pageNums := make([]PageID, 0, len(pageSets))
					for i := range pageSets { //map iterates over key not indices
						pageNums = append(pageNums, PageID(i))
					}
					slices.Sort(pageNums)
					cellRows := b.bufferPool.SelectDataPages(tmpTable.Name, pageNums)

					for i := range cellRows {
						if _, ok := values[cellRows[i][cond.operand1Index].AsInt()]; !ok {
							continue
						} else {
							delete(values, cellRows[i][cond.operand1Index].AsInt())
						}
						tmprows = append(tmprows, cellRows[i])
					}
				case Lt, Lte:
					potentialLeaf := tmpTable.indices.primaryTree.findLeaf(tmpTable.indices.primaryTree.root, num)
					firstLeaf := tmpTable.indices.primaryTree.findFirstLeaf()
					pageSets := make(map[int64]struct{})
					values := make(map[int64]struct{})
					for firstLeaf != nil && firstLeaf != potentialLeaf {
						for i := range firstLeaf.keys {
							if num == firstLeaf.keys[i] && cond.Operator == Lte {
								pageSets[firstLeaf.values[i]/PAGESIZE] = struct{}{}
								values[firstLeaf.keys[i]] = struct{}{}
							} else if firstLeaf.keys[i] < num {
								pageSets[firstLeaf.values[i]/PAGESIZE] = struct{}{}
								values[firstLeaf.keys[i]] = struct{}{}
							}
						}
						firstLeaf = firstLeaf.next
					}
					pageNums := make([]PageID, 0, len(pageSets))
					for i := range pageSets { //map iterates over key not indices
						pageNums = append(pageNums, PageID(i))
					}
					slices.Sort(pageNums)
					cellRows := b.bufferPool.SelectDataPages(tmpTable.Name, pageNums)

					for i := range cellRows {
						if _, ok := values[cellRows[i][cond.operand1Index].AsInt()]; !ok {
							continue
						} else {
							delete(values, cellRows[i][cond.operand1Index].AsInt())
						}
						tmprows = append(tmprows, cellRows[i])
					}
				case Ne: //iterate over all rows and just remove one not needed
					startPage := PageID(0)
					endPage := PageID(tmpTable.lastPage)
					cellRows := b.bufferPool.SelectDataRange(tmpTable.Name, startPage, endPage)
					for i := range cellRows {
						if num == cellRows[i][cond.operand1Index].AsInt() {
							continue
						}
						tmprows = append(tmprows, cellRows[i])
					}
				}
				tmpConditions = removeColFieldGen(tmpConditions, i)
				hasIndex = true
				break
			}
		}
		if !hasIndex { //fetch all rows
			startPage := PageID(0)
			endPage := PageID(tmpTable.lastPage)
			cellRows := b.bufferPool.SelectDataRange(tmpTable.Name, startPage, endPage)
			tmprows = cellRows
		}

		for i := range tmprows {
			row := tmprows[i]
			passes := true
		condLoop:
			for j := range tmpConditions {
				cond := tmpConditions[j]
				switch tmpTable.Columns[cond.operand1Index].columnType {
				case INT:
					var leftVal int64 = row[cond.operand1Index].AsInt()
					var rightVal int64
					if cond.Operand2IsField {
						rightVal = row[cond.operand2Index].AsInt()
					} else {
						rightVal, _ = strconv.ParseInt(cond.Operand2, 10, 64)
					}
					switch cond.Operator {
					case Eq:
						if !(leftVal == rightVal) {
							passes = false
							break condLoop
						}
					case Ne:
						if leftVal == rightVal {
							passes = false
							break condLoop
						}
					case Gt:
						if !(leftVal > rightVal) {
							passes = false
							break condLoop
						}
					case Lt:
						if !(leftVal < rightVal) {
							passes = false
							break condLoop
						}
					case Gte:
						if !(leftVal >= rightVal) {
							passes = false
							break condLoop
						}
					case Lte:
						if !(leftVal <= rightVal) {
							passes = false
							break condLoop
						}
					}
				case FLOAT:
					var leftVal float64 = row[cond.operand1Index].AsFloat()
					var rightVal float64
					if cond.Operand2IsField {
						rightVal = row[cond.operand2Index].AsFloat()
					} else {
						rightVal, _ = strconv.ParseFloat(cond.Operand2, 64)
					}
					switch cond.Operator {
					case Eq:
						if !(leftVal == rightVal) {
							passes = false
							break condLoop
						}
					case Ne:
						if leftVal == rightVal {
							passes = false
							break condLoop
						}
					case Gt:
						if !(leftVal > rightVal) {
							passes = false
							break condLoop
						}
					case Lt:
						if !(leftVal < rightVal) {
							passes = false
							break condLoop
						}
					case Gte:
						if !(leftVal >= rightVal) {
							passes = false
							break condLoop
						}
					case Lte:
						if !(leftVal <= rightVal) {
							passes = false
							break condLoop
						}
					}
				case BOOL:
					var leftVal bool = row[cond.operand1Index].AsBool()
					var rightVal bool
					if cond.Operand2IsField {
						rightVal = row[cond.operand2Index].AsBool()
					} else {
						rightVal, _ = strconv.ParseBool(cond.Operand2)
					}
					switch cond.Operator {
					case Eq:
						if !(leftVal == rightVal) {
							passes = false
							break condLoop
						}
					case Ne:
						if leftVal == rightVal {
							passes = false
							break condLoop
						}
					}
				case CHAR:
					var leftVal string = row[cond.operand1Index].AsString()
					var rightVal string
					if cond.Operand2IsField {
						rightVal = row[cond.operand2Index].AsString()
					} else {
						rightVal = cond.Operand2
					}
					switch cond.Operator {
					case Eq:
						if !(leftVal == rightVal) {
							passes = false
							break condLoop
						}
					case Ne:
						if leftVal == rightVal {
							passes = false
							break condLoop
						}
					case Gt:
						if !(leftVal > rightVal) {
							passes = false
							break condLoop
						}
					case Lt:
						if !(leftVal < rightVal) {
							passes = false
							break condLoop
						}
					case Gte:
						if !(leftVal >= rightVal) {
							passes = false
							break condLoop
						}
					case Lte:
						if !(leftVal <= rightVal) {
							passes = false
							break condLoop
						}
					}
				}
			}
			if passes {
				rows.rows = append(rows.rows, row)
			}
		}
	}

	return rows, nil
}

func (b *Backend) Close() {
	b.bufferPool.close()
	for i := range b.tables {
		b.tables[i].indices.close()
	}
}

func (b *Backend) checkTableExist(q Query) (*Table, bool) {
	for i := range b.tables {
		if q.TableName == b.tables[i].Name {
			return &b.tables[i], true
		}
	}
	return nil, false
}

func removeColFieldGen[T any](s []T, i int) []T {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func intersectGeneric[T comparable](a []T, b []T) []T {
	set := make([]T, 0, max(len(a), len(b)))

	for _, v := range a {
		if containsGeneric(b, v) {
			set = append(set, v)
		}
	}

	return set
}

func containsGeneric[T comparable](b []T, e T) bool {
	for _, v := range b {
		if v == e {
			return true
		}
	}
	return false
}
