package token

type TokenType string

type Token struct {
	Type    TokenType
	Literal string
}

// TODO:convert to enum

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"
	// Identifiers + literals
	IDENT         = "IDENT" // add, foobar, x, y, ...
	STRINGLITERAL = "STRINGLITERAL"
	NUMBERLITERAL = "NUMBERLITERAL"
	BOOLLITERAL   = "BOOLLITERAL"
	// Data types
	INT   = "INT" // 1343456
	CHAR  = "CHAR"
	BOOL  = "BOOL"
	FLOAT = "FLOAT"
	// Operators
	PLUS     = "+"
	MINUS    = "-"
	BANG     = "!"
	ASTERISK = "*"
	SLASH    = "/"
	LT       = "<"
	GT       = ">"
	LTE      = "<="
	GTE      = ">="
	EQ       = "="
	NOT_EQ   = "!="
	// Delimiters
	COMMA     = ","
	SEMICOLON = ";"
	LPAREN    = "("
	RPAREN    = ")"
	PERIOD    = "."
	// Keywords
	SELECT = "SELECT"
	INSERT = "INSERT"
	INTO   = "INTO"
	VALUES = "VALUES"
	UPDATE = "UPDATE"
	DELETE = "DELETE"
	CREATE = "CREATE"
	DROP   = "DROP"
	TABLE  = "TABLE"
	FROM   = "FROM"
	WHERE  = "WHERE"
	SET    = "SET"
	AS     = "AS"
	IS     = "IS"
	TRUE   = "TRUE"
	FALSE  = "FALSE"
	AND    = "AND"
	// Constraints
	PRIMARY = "PRIMARY"
	KEY     = "KEY"
	NOT     = "NOT"
	NULL    = "NULL"
	UNIQUE  = "UNIQUE"
)

var keywords = map[string]TokenType{
	"SELECT":  SELECT,
	"INSERT":  INSERT,
	"INTO":    INTO,
	"VALUES":  VALUES,
	"UPDATE":  UPDATE,
	"DELETE":  DELETE,
	"FROM":    FROM,
	"WHERE":   WHERE,
	"SET":     SET,
	"AS":      AS,
	"CREATE":  CREATE,
	"TABLE":   TABLE,
	"DROP":    DROP,
	"AND":     AND,
	"PRIMARY": PRIMARY,
	"KEY":     KEY,
	"NOT":     NOT,
	"NULL":    NULL,
	"UNIQUE":  UNIQUE,
	"INT":     INT,
	"FLOAT":   FLOAT,
	"TRUE":    BOOLLITERAL,
	"FALSE":   BOOLLITERAL,
	"CHAR":    CHAR,
	"BOOL":    BOOL,
}

var dataTypes = map[TokenType]struct{}{
	INT:   {},
	FLOAT: {},
	BOOL:  {},
	CHAR:  {},
}

var constraintTypes = map[TokenType]struct{}{
	"PRIMARY KEY": {}, "NOT NULL": {}, "UNIQUE": {}, "NULL": {},
}

// var reservedConstraints = []string{
// 	"PRIMARY KEY", "NOT NULL", "UNIQUE",
// }

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENT
}

func LookupDataType(tok TokenType) bool {
	_, ok := dataTypes[tok]
	return ok
}

func LookupConstraint(tok TokenType) bool {
	_, ok := constraintTypes[tok]
	return ok
}
