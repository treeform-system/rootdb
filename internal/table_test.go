package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTable(t *testing.T) {
	var tests = []struct {
		name  string
		input Table
	}{
		// the table itself
		{"Table equality test 1",
			Table{
				Name:      "MyTable2024",
				lastRowId: 50,
				Columns: []Column{
					{columnName: "SomeName", columnType: BOOL, columnSize: 8, columnIsUnique: false, columnIsNullable: true, columnIsPrimary: false},
					{columnName: "_column1", columnType: FLOAT, columnSize: 24, columnIsUnique: false, columnIsNullable: false, columnIsPrimary: true},
					{columnName: "New_column", columnType: INT, columnSize: 17, columnIsUnique: true, columnIsNullable: false, columnIsPrimary: false},
				},
			}},
		{"Table equality test 2",
			Table{
				Name:      "someTable@54",
				lastRowId: 655367,
				Columns: []Column{
					{columnName: "Testing", columnType: CHAR, columnSize: 8, columnIsUnique: true, columnIsNullable: true, columnIsPrimary: false},
					{columnName: "_mycol", columnType: FLOAT, columnSize: 56, columnIsUnique: false, columnIsNullable: true, columnIsPrimary: false},
					{columnName: "randColumn123", columnType: CHAR, columnSize: 255, columnIsUnique: true, columnIsNullable: false, columnIsPrimary: true},
				},
			}},
		{"Table equality test 3",
			Table{
				Name:      "Table3",
				lastRowId: 1,
				Columns: []Column{
					{columnName: "someCol@", columnType: INT, columnSize: 1, columnIsUnique: true, columnIsNullable: true, columnIsPrimary: true},
					{columnName: "FUNCOLUMN", columnType: FLOAT, columnSize: 3, columnIsUnique: true, columnIsNullable: false, columnIsPrimary: false},
					{columnName: "New_col", columnType: INT, columnSize: 26, columnIsUnique: true, columnIsNullable: false, columnIsPrimary: false},
					{columnName: "lastcol", columnType: BOOL, columnSize: 18, columnIsUnique: false, columnIsNullable: false, columnIsPrimary: false},
				},
			}},
	}
	// The execution loop
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.input.toBytes()
			tt.input.GenerateFields()

			newtable := Table{}
			readLen := newtable.fromBytes(buf)
			if readLen != len(buf) {
				t.Error("len of bytes from column not equal to read length from 'frombytes'")
			}
			newtable.GenerateFields()
			require.Equal(t, tt.input, newtable)
		})
	}
}
