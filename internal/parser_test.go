package internal

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type testCase struct {
	Name     string
	SQL      string
	Expected Query
	Err      error
}

func TestSelectSQL(t *testing.T) {
	ts := []testCase{
		{
			Name:     "empty query fails",
			SQL:      "",
			Expected: Query{},
			Err:      fmt.Errorf("query type cannot be empty"),
		},
		{
			Name:     "SELECT without FROM fails",
			SQL:      "SELECT;",
			Expected: Query{Type: Select},
			Err:      fmt.Errorf("at SELECT: expected field to SELECT"),
		},
		{
			Name:     "SELECT without fields fails",
			SQL:      "SELECT FROM a;",
			Expected: Query{Type: Select},
			Err:      fmt.Errorf("at SELECT: expected field to SELECT"),
		},
		{
			Name:     "SELECT with comma and empty field fails",
			SQL:      "SELECT b, FROM a;",
			Expected: Query{Type: Select},
			Err:      fmt.Errorf("at SELECT: expected field to SELECT"),
		},
		{
			Name:     "SELECT works",
			SQL:      "SELECT a FROM b;",
			Expected: Query{Type: Select, TableName: "b", Fields: []string{"a"}},
			Err:      nil,
		},
		{
			Name:     "SELECT works with lowercase",
			SQL:      "select a fRoM \"b\";",
			Expected: Query{Type: Select, TableName: "b", Fields: []string{"a"}},
			Err:      nil,
		},
		{
			Name:     "SELECT many fields works",
			SQL:      "SELECT a, c, d FROM b;",
			Expected: Query{Type: Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			Err:      nil,
		},
		{
			Name: "SELECT with alias works",
			SQL:  "SELECT a as z, b as y, c FROM \"b\";",
			Expected: Query{
				Type:      Select,
				TableName: "b",
				Fields:    []string{"a", "b", "c"},
				Aliases: map[string]string{
					"a": "z",
					"b": "y",
				},
			},
			Err: nil,
		},
		{
			Name:     "SELECT with empty WHERE fails",
			SQL:      "SELECT a, c, d FROM b WHERE;",
			Expected: Query{Type: Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			Err:      fmt.Errorf("at WHERE: expected field"),
		},
		{
			Name:     "SELECT with WHERE with only operand fails",
			SQL:      "SELECT a, c, d FROM b WHERE a;",
			Expected: Query{Type: Select, TableName: "b", Fields: []string{"a", "c", "d"}},
			Err:      fmt.Errorf("at WHERE: unknown operator"),
		},
		{
			Name: "SELECT with WHERE with = works",
			SQL:  "SELECT a, c, d FROM b WHERE a = '';",
			Expected: Query{
				Type:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Eq, Operand2: "", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with < works",
			SQL:  "SELECT a, c, d FROM \"b\" WHERE a < 1;",
			Expected: Query{
				Type:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Lt, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with <= works",
			SQL:  "SELECT a, c, d FROM b WHERE a <= 1;",
			Expected: Query{
				Type:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Lte, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with boolean value works",
			SQL:  "SELECT a, c, d FROM b WHERE a = true;",
			Expected: Query{
				Type:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Eq, Operand2: "TRUE", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with > works",
			SQL:  "SELECT a, c, d FROM b WHERE a > 1;",
			Expected: Query{
				Type:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Gt, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with >= works",
			SQL:  "SELECT a, c, d FROM b WHERE a >= 1;",
			Expected: Query{
				Type:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Gte, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with != works",
			SQL:  "SELECT a, c,	 d FROM b WHERE a != '1';",
			Expected: Query{
				Type:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Ne, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with != works (comparing field against another field)",
			SQL:  "SELECT a, c, d FROM b WHERE a != \"b\";",
			Expected: Query{
				Type:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Ne, Operand2: "b", Operand2IsField: true},
				},
			},
			Err: nil,
		},
		{
			Name: "SELECT * works",
			SQL:  "SELECT * FROM b;",
			Expected: Query{
				Type:       Select,
				TableName:  "b",
				Fields:     []string{"*"},
				Conditions: nil,
			},
			Err: nil,
		},
		{
			Name: "SELECT a, * works",
			SQL:  "SELECT a, * FROM b;",
			Expected: Query{
				Type:       Select,
				TableName:  "b",
				Fields:     []string{"a", "*"},
				Conditions: nil,
			},
			Err: nil,
		},
		{
			Name: "SELECT with WHERE with two conditions using AND works",
			SQL:  "SELECT a, c, d FROM b WHERE a != 1 AND b = '2';",
			Expected: Query{
				Type:      Select,
				TableName: "b",
				Fields:    []string{"a", "c", "d"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Ne, Operand2: "1", Operand2IsField: false},
					{Operand1: "b", Operand1IsField: true, Operator: Eq, Operand2: "2", Operand2IsField: false},
				},
			},
			Err: nil,
		},
	}

	for _, tc := range ts {
		t.Run(tc.Name, func(t *testing.T) {
			_, err := Parse(tc.SQL)
			if tc.Err != nil && err == nil {
				t.Errorf("Error should have been %v", tc.Err)
			}
			if tc.Err == nil && err != nil {
				t.Errorf("Error should have been nil but was %v", err)
			}
			if tc.Err != nil && err != nil {
				require.Equal(t, tc.Err, err, "Unexpected error")
			}
		})
	}
}

func TestUpdateSQL(t *testing.T) {
	ts := []testCase{
		{
			Name:     "Empty UPDATE fails",
			SQL:      "UPDATE;",
			Expected: Query{},
			Err:      fmt.Errorf("at UPDATE: expected table name"),
		},
		{
			Name:     "Incomplete UPDATE with table name fails",
			SQL:      "UPDATE a;",
			Expected: Query{},
			Err:      fmt.Errorf("at UPDATE: expected 'SET'"),
		},
		{
			Name:     "Incomplete UPDATE with table name and SET fails",
			SQL:      "UPDATE a SET;",
			Expected: Query{},
			Err:      fmt.Errorf("at UPDATE: expected at least one field to update"),
		},
		{
			Name:     "Incomplete UPDATE with table name, SET with a field but no value and WHERE fails",
			SQL:      "UPDATE a SET b WHERE;",
			Expected: Query{},
			Err:      fmt.Errorf("at UPDATE: expected '='"),
		},
		{
			Name:     "Incomplete UPDATE with table name, SET with a field and = but no value and WHERE fails",
			SQL:      "UPDATE a SET b = WHERE;",
			Expected: Query{},
			Err:      fmt.Errorf("at UPDATE: expected value for update"),
		},
		{
			Name:     "Incomplete UPDATE due to no WHERE clause fails",
			SQL:      "UPDATE a SET b = 'hello' WHERE;",
			Expected: Query{},
			Err:      fmt.Errorf("at WHERE: expected field"),
		},
		{
			Name:     "Incomplete UPDATE due incomplete WHERE clause fails",
			SQL:      "UPDATE a SET b = 'hello' WHERE a;",
			Expected: Query{},
			Err:      fmt.Errorf("at WHERE: unknown operator"),
		},
		{
			Name: "UPDATE works",
			SQL:  "UPDATE a SET \"b\" = 'hello' WHERE a = 1;",
			Expected: Query{
				Type:      Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "UPDATE works with simple quote inside",
			SQL:  "UPDATE a SET b = 'hello\\world' WHERE a = 1;",
			Expected: Query{
				Type:      Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello\\world"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "UPDATE with multiple SETs works",
			SQL:  "UPDATE a SET b = 'hello', c = 'bye' WHERE a = 1;",
			Expected: Query{
				Type:      Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello", "c": "bye"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Eq, Operand2: "1", Operand2IsField: false},
				},
			},
			Err: nil,
		},
		{
			Name: "UPDATE with multiple SETs and multiple conditions works",
			SQL:  "UPDATE a SET b = 'hello', c = 'bye' WHERE a = '1' AND b = 789;",
			Expected: Query{
				Type:      Update,
				TableName: "a",
				Updates:   map[string]string{"b": "hello", "c": "bye"},
				Conditions: []Condition{
					{Operand1: "a", Operand1IsField: true, Operator: Eq, Operand2: "1", Operand2IsField: false},
					{Operand1: "b", Operand1IsField: true, Operator: Eq, Operand2: "789", Operand2IsField: false},
				},
			},
			Err: nil,
		},
	}

	for _, tc := range ts {
		t.Run(tc.Name, func(t *testing.T) {
			_, err := Parse(tc.SQL)
			if tc.Err != nil && err == nil {
				t.Errorf("Error should have been %v", tc.Err)
			}
			if tc.Err == nil && err != nil {
				t.Errorf("Error should have been nil but was %v", err)
			}
			if tc.Err != nil && err != nil {
				require.Equal(t, tc.Err, err, "Unexpected error")
			}
		})
	}
}

func TestDeleteSQL(t *testing.T) {
	ts := []testCase{
		{
			Name:     "Empty DELETE fails",
			SQL:      "DELETE FROM;",
			Expected: Query{},
			Err:      fmt.Errorf("at DELETE FROM: expected table name"),
		},
		{
			Name:     "DELETE without WHERE fails",
			SQL:      "DELETE FROM \"a\";",
			Expected: Query{},
			Err:      fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE"),
		},
		{
			Name:     "DELETE with empty WHERE fails",
			SQL:      "DELETE FROM a WHERE;",
			Expected: Query{},
			Err:      fmt.Errorf("at WHERE: expected field"),
		},
		{
			Name:     "DELETE with WHERE with field but no operator fails",
			SQL:      "DELETE FROM a WHERE b;",
			Expected: Query{},
			Err:      fmt.Errorf("at WHERE: unknown operator"),
		},
		{
			Name: "DELETE with WHERE works",
			SQL:  "DELETE FROM a WHERE b = 'some';",
			Expected: Query{
				Type:      Delete,
				TableName: "a",
				Conditions: []Condition{
					{Operand1: "b", Operand1IsField: true, Operator: Eq, Operand2: "some", Operand2IsField: false},
				},
			},
			Err: nil,
		},
	}

	for _, tc := range ts {
		t.Run(tc.Name, func(t *testing.T) {
			_, err := Parse(tc.SQL)
			if tc.Err != nil && err == nil {
				t.Errorf("Error should have been %v", tc.Err)
			}
			if tc.Err == nil && err != nil {
				t.Errorf("Error should have been nil but was %v", err)
			}
			if tc.Err != nil && err != nil {
				require.Equal(t, tc.Err, err, "Unexpected error")
			}
		})
	}
}

func TestInsertSQL(t *testing.T) {
	ts := []testCase{
		{
			Name:     "Empty INSERT fails",
			SQL:      "INSERT INTO;",
			Expected: Query{},
			Err:      fmt.Errorf("at INSERT INTO: expected table name"),
		},
		{
			Name:     "INSERT with no rows to insert fails",
			SQL:      "INSERT INTO a;",
			Expected: Query{},
			Err:      fmt.Errorf("at INSERT INTO: expected opening parens"),
		},
		{
			Name:     "INSERT with incomplete value section fails",
			SQL:      "INSERT INTO a (;",
			Expected: Query{},
			Err:      fmt.Errorf("at INSERT INTO: expected at least one field to insert"),
		},
		{
			Name:     "INSERT with incomplete value section fails #2",
			SQL:      "INSERT INTO a (b;",
			Expected: Query{},
			Err:      fmt.Errorf("at INSERT INTO: expected comma or closing parens"),
		},
		{
			Name:     "INSERT with incomplete value section fails #3",
			SQL:      "INSERT INTO a (b);",
			Expected: Query{},
			Err:      fmt.Errorf("at INSERT INTO: expected 'VALUES'"),
		},
		{
			Name:     "INSERT with incomplete value section fails #4",
			SQL:      "INSERT INTO a (b) VALUES;",
			Expected: Query{},
			Err:      fmt.Errorf("at INSERT INTO: expected opening parens"),
		},
		{
			Name:     "INSERT with incomplete row fails",
			SQL:      "INSERT INTO a (b) VALUES (;",
			Expected: Query{},
			Err:      fmt.Errorf("at INSERT INTO: expected value to insert string or number literal"),
		},
		{
			Name: "INSERT works",
			SQL:  "INSERT INTO a (b) VALUES ('1');",
			Expected: Query{
				Type:      Insert,
				TableName: "a",
				Fields:    []string{"b"},
				Inserts:   [][]string{{"1"}},
			},
			Err: nil,
		},
		{
			Name:     "INSERT * fails",
			SQL:      "INSERT INTO a (*) VALUES ('1');",
			Expected: Query{},
			Err:      fmt.Errorf("at INSERT INTO: expected at least one field to insert"),
		},
		{
			Name: "INSERT with multiple fields works",
			SQL:  "INSERT INTO a (b,c,    d) VALUES ('1','2' ,  '3' );",
			Expected: Query{
				Type:      Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]string{{"1", "2", "3"}},
			},
			Err: nil,
		},
		{
			Name: "INSERT with multiple fields and multiple values works",
			SQL:  "INSERT INTO a (b,c,    d) VALUES ('1','2' ,  '3' ),('4','5' ,'6' );",
			Expected: Query{
				Type:      Insert,
				TableName: "a",
				Fields:    []string{"b", "c", "d"},
				Inserts:   [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
			},
			Err: nil,
		},
	}

	for _, tc := range ts {
		t.Run(tc.Name, func(t *testing.T) {
			_, err := Parse(tc.SQL)
			if tc.Err != nil && err == nil {
				t.Errorf("Error should have been %v", tc.Err)
			}
			if tc.Err == nil && err != nil {
				t.Errorf("Error should have been nil but was %v", err)
			}
			if tc.Err != nil && err != nil {
				require.Equal(t, tc.Err, err, "Unexpected error")
			}
		})
	}
}

func TestCreateSQL(t *testing.T) {
	ts := []testCase{
		{
			Name:     "Empty CREATE fails",
			SQL:      "CREATE TABLE;",
			Expected: Query{},
			Err:      fmt.Errorf("at CREATE TABLE: expected quoted table name"),
		},
		{
			Name:     "Empty CREATE table",
			SQL:      "CREATE TABLE a;",
			Expected: Query{},
			Err:      fmt.Errorf("at CREATE TABLE: expected opening parens"),
		},
		{
			Name:     "CREATE table with no fields",
			SQL:      "CREATE TABLE b ();",
			Expected: Query{},
			Err:      fmt.Errorf("at CREATE TABLE: expected field to CREATE"),
		},
		{
			Name:     "CREATE with field no datatype",
			SQL:      "CREATE TABLE b (ID);",
			Expected: Query{},
			Err:      fmt.Errorf("at CREATE TABLE: expected valid data type for column"),
		},
		{
			Name: "CREATE with valid field and datatype",
			SQL:  "CREATE TABLE b (ID int);",
			Expected: Query{
				Type:      Create,
				TableName: "b",
				TableConstruction: createQuery{
					fieldsWTypes: [][]string{{"ID", "INT"}},
				},
			},
			Err: nil,
		},
		{
			Name: "CREATE with multiple fields",
			SQL:  "CREATE TABLE c10 (column1 int, column2 char(20),column3 bool, column4 float);",
			Expected: Query{
				Type:      Create,
				TableName: "c10",
				TableConstruction: createQuery{
					fieldsWTypes: [][]string{{"column1", "INT"}, {"column2", "CHAR", "20"}, {"column3", "BOOL"}, {"column4", "FLOAT"}},
				},
			},
			Err: nil,
		},
		{
			Name: "CREATE with valid field and datatype with constraints",
			SQL:  "CREATE TABLE b(ID int Primary Key);",
			Expected: Query{
				Type:      Create,
				TableName: "b",
				TableConstruction: createQuery{
					fieldsWTypes: [][]string{{"ID", "INT"}},
					primary:      []string{"ID"},
				},
			},
			Err: nil,
		},
		{
			Name: "CREATE multiple valid fields and datatype with constraints",
			SQL:  "CREATE TABLE b (ID int Primary Key,Column flOat UNique NOT NULL,third Char(50) NOt Null);",
			Expected: Query{
				Type:      Create,
				TableName: "b",
				TableConstruction: createQuery{
					fieldsWTypes: [][]string{{"ID", "INT"}, {"Column", "FLOAT"}, {"third", "CHAR", "50"}},
					primary:      []string{"ID"},
					unique:       []string{"Column"},
					notnullable:  []string{"Column", "third"},
				},
			},
			Err: nil,
		},
		{
			Name: "CREATE multiple valid fields and datatype",
			SQL:  "CREATE TABLE MyTable10 (column1 int Primary Key, column2 char(20),column30 bool, column400 float);",
			Expected: Query{
				Type:      Create,
				TableName: "MyTable10",
				TableConstruction: createQuery{
					fieldsWTypes: [][]string{{"column1", "INT"}, {"column2", "CHAR", "20"}, {"column30", "BOOL"}, {"column400", "FLOAT"}},
					primary:      []string{"column1"},
				},
			},
			Err: nil,
		},
	}

	for _, tc := range ts {
		t.Run(tc.Name, func(t *testing.T) {
			_, err := Parse(tc.SQL)
			if tc.Err != nil && err == nil {
				t.Errorf("Error should have been %v", tc.Err)
			}
			if tc.Err == nil && err != nil {
				t.Errorf("Error should have been nil but was %v", err)
			}
			if tc.Err != nil && err != nil {
				require.Equal(t, tc.Err, err, "Unexpected error")
			}
		})
	}
}

func TestDropSQL(t *testing.T) {
	ts := []testCase{
		{
			Name:     "Empty DROP fails",
			SQL:      "DROP TABLE;",
			Expected: Query{},
			Err:      fmt.Errorf("at DROP TABLE: expected table name"),
		},
		{
			Name: "DROP TABLE with quotes",
			SQL:  "DROP TABLE \"mytable\";",
			Expected: Query{
				Type:      Drop,
				TableName: "mytable",
			},
			Err: nil,
		},
		{
			Name: "DROP TABLE with no quotes",
			SQL:  "DROP TABLE sometable;",
			Expected: Query{
				Type:      Drop,
				TableName: "sometable",
			},
			Err: nil,
		},
	}

	for _, tc := range ts {
		t.Run(tc.Name, func(t *testing.T) {
			_, err := Parse(tc.SQL)
			if tc.Err != nil && err == nil {
				t.Errorf("Error should have been %v", tc.Err)
			}
			if tc.Err == nil && err != nil {
				t.Errorf("Error should have been nil but was %v", err)
			}
			if tc.Err != nil && err != nil {
				require.Equal(t, tc.Err, err, "Unexpected error")
			}
		})
	}
}
