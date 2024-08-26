package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnConversion(t *testing.T) {
	var tests = []struct {
		name  string
		input Column
	}{
		{"Column equality test 1",
			Column{
				columnName:       "SomeName",
				columnType:       BOOL,
				columnSize:       8,
				columnIsUnique:   true,
				columnIsNullable: true,
				columnIsPrimary:  false,
			}},
		{"Column equality test 2",
			Column{
				columnName:       "_column1",
				columnType:       FLOAT,
				columnSize:       24,
				columnIsUnique:   false,
				columnIsNullable: false,
				columnIsPrimary:  false,
			}},
		{"Column equality test 3",
			Column{
				columnName:       "New_column",
				columnType:       INT,
				columnSize:       17,
				columnIsUnique:   true,
				columnIsNullable: false,
				columnIsPrimary:  true,
			}},
		{"Column equality test 4",
			Column{
				columnName:       "Col1234",
				columnType:       CHAR,
				columnSize:       24,
				columnIsUnique:   false,
				columnIsNullable: true,
				columnIsPrimary:  true,
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colBytes := tt.input.toBytes()
			newCol := Column{}
			readLen := newCol.fromBytes(colBytes)
			if readLen != len(colBytes) {
				t.Error("len of bytes from column not equal to read length from 'frombytes'")
			}
			require.Equal(t, tt.input, newCol)
		})
	}
}
