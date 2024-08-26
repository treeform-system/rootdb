package internal

type Query struct {
	Type              QueryType
	TableName         string
	Conditions        []Condition
	Updates           map[string]string
	Inserts           [][]string
	Fields            []string // Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names) and CREATE
	Aliases           map[string]string
	TableConstruction createQuery
}

type createQuery struct {
	fieldsWTypes [][]string //holds field names and type and optionally size
	nullable     []string
	notnullable  []string
	primary      []string
	unique       []string
}

type QueryType int

const (
	// UnknownType is the zero value for a queryType
	UnknownType QueryType = iota
	// Select represents a SELECT query
	Select
	// Update represents an UPDATE query
	Update
	// Insert represents an INSERT query
	Insert
	// Delete represents a DELETE query
	Delete
	// Create represents a CREATE query
	Create
	//Drop represents a DROP query
	Drop
)

// Operator is between operands in a condition
type Operator int

const (
	// UnknownOperator is the zero value for an Operator
	UnknownOperator Operator = iota
	Eq                       // Eq -> "="
	Ne                       // Ne -> "!="
	Gt                       // Gt -> ">"
	Lt                       // Lt -> "<"
	Gte                      // Gte -> ">="
	Lte                      // Lte -> "<="
)

// Condition is a single boolean condition in a WHERE clause
type Condition struct {
	// Operand1 is the left hand side operand
	Operand1 string
	// Operand1IsField determines if Operand1 is a literal or a field name
	Operand1IsField bool
	// Operator is e.g. "=", ">"
	Operator Operator
	// Operand1 is the right hand side operand
	Operand2 string
	// Operand2IsField determines if Operand2 is a literal or a field name
	Operand2IsField bool
}
