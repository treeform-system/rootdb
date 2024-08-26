package internal

const (
	COL_ISUNIQUE  = 1 << 0
	COL_ISNULL    = 1 << 1
	COL_ISPRIMARY = 1 << 2
)

// ColumnType values are define in parser.go
//
// Column values should always immutable after first creation
// TODO: change allocate buffer to not use columnoffset
type Column struct {
	columnName       string //max size is maxuint8
	columnType       uint8  //data type
	columnSize       uint8  //size of column in database in bytes
	columnIsUnique   bool
	columnIsNullable bool
	columnIsPrimary  bool
	columnOffset     int //dynamic at runtime
	columnIndex      int //dynamic at runtime
}

func (c *Column) toBytes() []byte {
	allbytes := make([]byte, 1, 40+16)
	allbytes[0] = uint8(len(c.columnName))
	allbytes = append(allbytes, c.columnName...)
	constraintNum := byte(0)
	if c.columnIsUnique {
		constraintNum |= COL_ISUNIQUE
	}
	if c.columnIsNullable {
		constraintNum |= COL_ISNULL
	}
	if c.columnIsPrimary {
		constraintNum |= COL_ISPRIMARY
	}
	allbytes = append(allbytes, c.columnType, c.columnSize, constraintNum)
	return allbytes
}

// assumes valid byte slice passed with full information for column
//
// returns number of bytes read
func (c *Column) fromBytes(colBytes []byte) int {
	offset := 1
	lengthName := colBytes[0]
	c.columnName = string(colBytes[offset : offset+int(lengthName)])
	offset += int(lengthName)
	c.columnType = colBytes[offset]
	offset++
	c.columnSize = colBytes[offset]
	offset++
	constraintNum := colBytes[offset]
	offset++
	c.columnIsUnique = (constraintNum & COL_ISUNIQUE) > 0
	c.columnIsNullable = (constraintNum & COL_ISNULL) > 0
	c.columnIsPrimary = (constraintNum & COL_ISPRIMARY) > 0
	return offset
}

type ResultColumn struct {
	ColumnType uint8
	Name       string
	columnPos  int
}
type insertType uint8

const (
	COL_I_PRIMARYNULL insertType = iota
	COL_I_PRIMARYVALUED
	COL_I_VALUED
	COL_I_NULL
)

type InsertColumn struct {
	columnSize  uint8
	colType     insertType
	dataType    uint8
	insertIndex int //index of incoming Values slice
}
