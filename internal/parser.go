package internal

import (
	"errors"
	"fmt"
	"strings"

	"github.com/treeform-system/rootdb/internal/token"
)

// '' is a string literal while "" is an identifier "" not required for identifier only if characters not valid

// Parse takes a string representing SQl query and parses it into a query.Query struct. May fail.
func Parse(sql string) (Query, error) {
	q, err := parse(sql)
	if err != nil {
		return q, err
	}
	return q, nil
}

func parse(sql string) (Query, error) {
	return (&parser{sql: strings.TrimSpace(sql), step: stepType, query: Query{}, nextUpdateField: ""}).parse()
}

type step int

const (
	stepType step = iota
	stepSelectField
	stepSelectFrom
	stepSelectComma
	stepSelectFromTable
	stepInsertTable
	stepInsertFieldsOpeningParens
	stepInsertFields
	stepInsertFieldsCommaOrClosingParens
	stepInsertValuesOpeningParens
	stepInsertValuesWord
	stepInsertValues
	stepInsertValuesCommaOrClosingParens
	stepInsertValuesCommaBeforeOpeningParens
	stepUpdateTable
	stepUpdateSet
	stepUpdateField
	stepUpdateEquals
	stepUpdateValue
	stepUpdateComma
	stepDeleteFromTable
	stepWhere
	stepWhereField
	stepWhereOperator
	stepWhereValue
	stepWhereAnd
	stepCreateTable
	stepCreateFieldsOpeningParens
	stepCreateFields
	stepCreateColumnType
	stepCreateColumnSize
	stepCreateConstraints
	stepDropTable
)

type parser struct {
	i               int
	sql             string
	lexer           *Lexer
	curToken        token.Token
	peekToken       token.Token
	step            step
	query           Query
	err             error
	nextUpdateField string
}

const (
	INT = iota + 1
	FLOAT
	BOOL
	CHAR
)

func (p *parser) parse() (Query, error) {
	if p.sql == "" {
		return p.query, errors.New("query type cannot be empty")
	} else if p.sql[len(p.sql)-1] != ';' {
		return p.query, errors.New("sql string must end in semicolon")
	}

	p.lexer = NewLexer(p.sql)
	p.nextToken()
	p.nextToken()

	q, err := p.doParse()
	p.err = err
	if p.err == nil {
		p.err = p.validate()
	}
	p.logError()
	return q, p.err
}

func (p *parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

func (p *parser) doParse() (Query, error) {

	for p.curToken.Type != token.EOF {
		if p.curToken.Type == token.ILLEGAL {
			return p.query, errors.New("unknown token in sql string")
		}
		switch p.step {
		case stepType:
			switch p.curToken.Type {
			case token.SELECT:
				p.query.Type = Select
				p.step = stepSelectField
			case token.INSERT:
				p.query.Type = Insert
				p.nextToken()
				if p.curToken.Type != token.INTO {
					return p.query, errors.New("insert statement invalid at INTO")
				}
				p.step = stepInsertTable
			case token.UPDATE:
				p.query.Type = Update
				p.query.Updates = map[string]string{}
				p.step = stepUpdateTable
			case token.DELETE:
				p.query.Type = Delete
				p.nextToken()
				if p.curToken.Type != token.FROM {
					return p.query, errors.New("delete statement invalid at DELETE")
				}
				p.step = stepDeleteFromTable
			case token.CREATE:
				p.query.Type = Create
				p.nextToken()
				if p.curToken.Type != token.TABLE {
					return p.query, errors.New("create statement invalid at CREATE")
				}
				p.step = stepCreateTable
			case token.DROP:
				p.query.Type = Drop
				p.nextToken()
				if p.curToken.Type != token.TABLE {
					return p.query, errors.New("create statement invalid at CREATE")
				}
				p.step = stepDropTable
			default:
				return p.query, fmt.Errorf("invalid query type")
			}
		// Select steps
		case stepSelectField:
			if p.curToken.Type != token.IDENT && p.curToken.Type != token.ASTERISK {
				return p.query, fmt.Errorf("at SELECT: expected field to SELECT")
			}
			p.query.Fields = append(p.query.Fields, p.curToken.Literal)
			identifier := p.curToken.Literal
			if p.peekToken.Type == token.AS {
				p.nextToken()
				if p.peekToken.Type != token.IDENT {
					return p.query, fmt.Errorf("at SELECT: expected field alias for \"" + identifier + " as\" to SELECT")
				}
				if p.query.Aliases == nil {
					p.query.Aliases = make(map[string]string)
				}
				p.query.Aliases[identifier] = p.peekToken.Literal
				p.nextToken()
			}
			if p.peekToken.Type == token.FROM {
				p.step = stepSelectFrom
			} else {
				p.step = stepSelectComma
			}
		case stepSelectComma:
			if p.curToken.Type != token.COMMA {
				return p.query, fmt.Errorf("at SELECT: expected comma or FROM")
			}
			p.step = stepSelectField
		case stepSelectFrom:
			if p.curToken.Type != token.FROM {
				return p.query, fmt.Errorf("at SELECT: expected FROM")
			}
			p.step = stepSelectFromTable
		case stepSelectFromTable:
			if p.curToken.Type != token.IDENT {
				return p.query, fmt.Errorf("at SELECT: expected table name")
			}
			p.query.TableName = p.curToken.Literal
			p.step = stepWhere
		// Delete steps
		case stepDeleteFromTable:
			if p.curToken.Type != token.IDENT {
				return p.query, fmt.Errorf("at DELETE FROM: expected table name")
			}
			p.query.TableName = p.curToken.Literal
			p.step = stepWhere
		// Update steps
		case stepUpdateTable:
			if p.curToken.Type != token.IDENT {
				return p.query, fmt.Errorf("at UPDATE: expected table name")
			}
			p.query.TableName = p.curToken.Literal
			p.step = stepUpdateSet
		case stepUpdateSet:
			if p.curToken.Type != token.SET {
				return p.query, fmt.Errorf("at UPDATE: expected 'SET'")
			}
			p.step = stepUpdateField
		case stepUpdateField:
			if p.curToken.Type != token.IDENT {
				return p.query, fmt.Errorf("at UPDATE: expected at least one field to update")
			}
			p.nextUpdateField = p.curToken.Literal
			p.step = stepUpdateEquals
		case stepUpdateEquals:
			if p.curToken.Type != token.EQ {
				return p.query, fmt.Errorf("at UPDATE: expected '='")
			}
			p.step = stepUpdateValue
		case stepUpdateValue:
			if p.curToken.Type != token.STRINGLITERAL && p.curToken.Type != token.NUMBERLITERAL {
				return p.query, fmt.Errorf("at UPDATE: expected value for update")
			}
			p.query.Updates[p.nextUpdateField] = p.curToken.Literal
			p.nextUpdateField = ""
			if p.peekToken.Type == token.WHERE {
				p.step = stepWhere
			} else {
				p.step = stepUpdateComma
			}
		case stepUpdateComma:
			if p.curToken.Type == token.SEMICOLON {
				return p.query, p.err
			}
			if p.curToken.Type != token.COMMA {
				return p.query, fmt.Errorf("at UPDATE: expected ','")
			}
			p.step = stepUpdateField
		// Where steps
		case stepWhere: //jumped from selectFromTable and deleteFromTable
			if p.curToken.Type == token.SEMICOLON {
				p.nextToken()
				break
			}
			if p.curToken.Type != token.WHERE {
				return p.query, fmt.Errorf("expected WHERE")
			}
			p.step = stepWhereField
		case stepWhereField:
			if p.curToken.Type != token.IDENT {
				return p.query, fmt.Errorf("at WHERE: expected field")
			}
			p.query.Conditions = append(p.query.Conditions, Condition{Operand1: p.curToken.Literal, Operand1IsField: true})
			p.step = stepWhereOperator
		case stepWhereOperator:
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			switch p.curToken.Type {
			case token.EQ:
				currentCondition.Operator = Eq
			case token.GT:
				currentCondition.Operator = Gt
			case token.GTE:
				currentCondition.Operator = Gte
			case token.LT:
				currentCondition.Operator = Lt
			case token.LTE:
				currentCondition.Operator = Lte
			case token.NOT_EQ:
				currentCondition.Operator = Ne
			default:
				return p.query, fmt.Errorf("at WHERE: unknown operator")
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.step = stepWhereValue
		case stepWhereValue:
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			if p.curToken.Type == token.BOOLLITERAL {
				currentCondition.Operand2 = strings.ToUpper(p.curToken.Literal)
				currentCondition.Operand2IsField = false
			} else if p.curToken.Type == token.IDENT {
				currentCondition.Operand2 = p.curToken.Literal
				currentCondition.Operand2IsField = true
			} else {
				if p.curToken.Type != token.STRINGLITERAL && p.curToken.Type != token.NUMBERLITERAL {
					return p.query, fmt.Errorf("at WHERE: expected value")
				}
				currentCondition.Operand2 = p.curToken.Literal
				currentCondition.Operand2IsField = false
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			if p.peekToken.Type == token.SEMICOLON {
				return p.query, p.err
			}
			p.step = stepWhereAnd
		case stepWhereAnd:
			if p.curToken.Type != token.AND {
				return p.query, fmt.Errorf("at WHERE: expected AND")
			}
			p.step = stepWhereField
		// Insert steps
		case stepInsertTable:
			if p.curToken.Type != token.IDENT {
				return p.query, fmt.Errorf("at INSERT INTO: expected table name")
			}
			p.query.TableName = p.curToken.Literal
			p.step = stepInsertFieldsOpeningParens
		case stepInsertFieldsOpeningParens:
			if p.curToken.Type != token.LPAREN {
				return p.query, fmt.Errorf("at INSERT INTO: expected opening parens")
			}
			p.step = stepInsertFields
		case stepInsertFields:
			if p.curToken.Type != token.IDENT {
				return p.query, fmt.Errorf("at INSERT INTO: expected at least one field to insert")
			}
			p.query.Fields = append(p.query.Fields, p.curToken.Literal)
			p.step = stepInsertFieldsCommaOrClosingParens
		case stepInsertFieldsCommaOrClosingParens:
			if p.curToken.Type != token.COMMA && p.curToken.Type != token.RPAREN {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
			}
			if p.curToken.Type == token.COMMA {
				p.step = stepInsertFields
			} else {
				p.step = stepInsertValuesWord
			}
		case stepInsertValuesWord:
			if p.curToken.Type != token.VALUES {
				return p.query, fmt.Errorf("at INSERT INTO: expected 'VALUES'")
			}
			p.step = stepInsertValuesOpeningParens
		case stepInsertValuesOpeningParens:
			if p.curToken.Type != token.LPAREN {
				return p.query, fmt.Errorf("at INSERT INTO: expected opening parens")
			}
			p.query.Inserts = append(p.query.Inserts, []string{})
			p.step = stepInsertValues
		case stepInsertValues:
			if p.curToken.Type != token.STRINGLITERAL && p.curToken.Type != token.NUMBERLITERAL && p.curToken.Type != token.BOOLLITERAL {
				return p.query, fmt.Errorf("at INSERT INTO: expected value to insert string or number literal- (%s - %s)", p.curToken.Literal, p.curToken.Type)
			}
			p.query.Inserts[len(p.query.Inserts)-1] = append(p.query.Inserts[len(p.query.Inserts)-1], p.curToken.Literal)
			p.step = stepInsertValuesCommaOrClosingParens
		case stepInsertValuesCommaOrClosingParens:
			if p.curToken.Type != token.COMMA && p.curToken.Type != token.RPAREN {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
			}
			if p.curToken.Type == token.COMMA {
				p.step = stepInsertValues
				break
			}
			currentInsertRow := p.query.Inserts[len(p.query.Inserts)-1]
			if len(currentInsertRow) < len(p.query.Fields) {
				return p.query, fmt.Errorf("at INSERT INTO: value count doesn't match field count")
			}
			p.step = stepInsertValuesCommaBeforeOpeningParens
		case stepInsertValuesCommaBeforeOpeningParens:
			if p.curToken.Type == token.SEMICOLON { //end of inserting values
				return p.query, p.err
			}
			if p.curToken.Type != token.COMMA {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma")
			}
			p.step = stepInsertValuesOpeningParens
		// Create steps
		case stepCreateTable:
			if p.curToken.Type != token.IDENT {
				return p.query, fmt.Errorf("at CREATE TABLE: expected quoted table name")
			}
			p.query.TableName = p.curToken.Literal
			p.step = stepCreateFieldsOpeningParens
		case stepCreateFieldsOpeningParens:
			if p.curToken.Type != token.LPAREN {
				return p.query, fmt.Errorf("at CREATE TABLE: expected opening parens")
			}
			p.step = stepCreateFields
		case stepCreateFields:
			if p.curToken.Type != token.IDENT {
				return p.query, fmt.Errorf("at CREATE TABLE: expected field to CREATE")
			}
			p.query.Fields = append(p.query.Fields, p.curToken.Literal)
			p.query.TableConstruction.fieldsWTypes = append(p.query.TableConstruction.fieldsWTypes, []string{p.curToken.Literal})
			p.step = stepCreateColumnType
		case stepCreateColumnType:
			if !token.LookupDataType(p.curToken.Type) {
				return p.query, fmt.Errorf("at CREATE TABLE: expected valid data type for column")
			}
			lastpos := len(p.query.TableConstruction.fieldsWTypes) - 1
			p.query.TableConstruction.fieldsWTypes[lastpos] = append(p.query.TableConstruction.fieldsWTypes[lastpos], strings.ToUpper(p.curToken.Literal))
			if p.peekToken.Type == token.LPAREN {
				p.step = stepCreateColumnSize
				p.nextToken() //consumes the left parentheses
			} else if p.peekToken.Type == token.COMMA {
				p.step = stepCreateFields
				p.nextToken() //consumes the comma
			} else if p.peekToken.Type == token.RPAREN {
				p.nextToken()
				if p.peekToken.Type != token.SEMICOLON {
					return p.query, errors.New("at CREATE TABLE: expected semicolon to end sql")
				}
				return p.query, p.err
			} else {
				p.step = stepCreateConstraints
			}
		case stepCreateColumnSize:
			if p.curToken.Type != token.NUMBERLITERAL {
				return p.query, errors.New("at CREATE TABLE: expected number for datatype size")
			}
			lastpos := len(p.query.TableConstruction.fieldsWTypes) - 1
			p.query.TableConstruction.fieldsWTypes[lastpos] = append(p.query.TableConstruction.fieldsWTypes[lastpos], p.curToken.Literal)
			if p.peekToken.Type != token.RPAREN {
				return p.query, fmt.Errorf("at CREATE TABLE: expected closing parens for size value")
			}
			p.nextToken()
			if p.peekToken.Type == token.COMMA {
				p.step = stepCreateFields
				p.nextToken()
			} else if p.peekToken.Type == token.RPAREN {
				p.nextToken()
				if p.peekToken.Type != token.SEMICOLON {
					return p.query, errors.New("at CREATE TABLE: expected semicolon to end sql")
				}
				return p.query, p.err
			} else {
				p.step = stepCreateConstraints
			}
		case stepCreateConstraints:
			if p.curToken.Type == token.PRIMARY {
				p.nextToken()
				if p.curToken.Type != token.KEY {
					return p.query, fmt.Errorf("at CREATE TABLE: expected key after primary keyword")
				}
				p.query.TableConstruction.primary = append(p.query.TableConstruction.primary, p.query.Fields[len(p.query.Fields)-1])
			} else if p.curToken.Type == token.UNIQUE {
				p.query.TableConstruction.unique = append(p.query.TableConstruction.unique, p.query.Fields[len(p.query.Fields)-1])
			} else if p.curToken.Type == token.NOT {
				p.nextToken()
				if p.curToken.Type != token.NULL {
					return p.query, fmt.Errorf("at CREATE TABLE: expected null keyword after not keyword")
				}
				p.query.TableConstruction.notnullable = append(p.query.TableConstruction.notnullable, p.query.Fields[len(p.query.Fields)-1])
			} else if p.curToken.Type == token.NULL {
				p.query.TableConstruction.nullable = append(p.query.TableConstruction.nullable, p.query.Fields[len(p.query.Fields)-1])
			} else {
				return p.query, fmt.Errorf("at CREATE TABLE: expected constraint keyword")
			}

			if p.peekToken.Type == token.COMMA {
				p.step = stepCreateFields
				p.nextToken() //consumes comma
			} else if p.peekToken.Type == token.RPAREN {
				p.nextToken()
				if p.peekToken.Type != token.SEMICOLON {
					return p.query, errors.New("at CREATE TABLE: expected semicolon to end sql string")
				}
				return p.query, p.err
			} else {
				p.step = stepCreateConstraints
			}
		case stepDropTable:
			if p.curToken.Type != token.IDENT {
				return p.query, fmt.Errorf("at DROP TABLE: expected table name")
			}
			p.query.TableName = p.curToken.Literal
			if p.peekToken.Type != token.SEMICOLON {
				return p.query, errors.New("at DROP TABLE: missing semicolon after table name")
			}
			return p.query, p.err
		}
		p.nextToken()
	}
	return p.query, p.err
}

func (p *parser) validate() error {
	if len(p.query.Conditions) == 0 && p.step == stepWhereField {
		return fmt.Errorf("at WHERE: empty WHERE clause")
	} else if p.query.Type == UnknownType {
		return fmt.Errorf("query type cannot be empty")
	} else if p.query.TableName == "" {
		return fmt.Errorf("table name cannot be empty")
	} else if len(p.query.Conditions) == 0 && (p.query.Type == Update || p.query.Type == Delete) {
		return fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE")
	}
	for _, c := range p.query.Conditions {
		if c.Operator == UnknownOperator {
			return fmt.Errorf("at WHERE: condition without operator")
		}
		if c.Operand1 == "" && c.Operand1IsField {
			return fmt.Errorf("at WHERE: condition with empty left side operand")
		}
		if c.Operand2 == "" && c.Operand2IsField {
			return fmt.Errorf("at WHERE: condition with empty right side operand")
		}
	}
	if p.query.Type == Insert && len(p.query.Inserts) == 0 {
		return fmt.Errorf("at INSERT INTO: need at least one row to insert")
	}
	if p.query.Type == Insert {
		for _, i := range p.query.Inserts {
			if len(i) != len(p.query.Fields) {
				return fmt.Errorf("at INSERT INTO: value count doesn't match field count")
			}
		}
	}
	if p.query.Type == Create && len(p.query.Fields) == 0 {
		return fmt.Errorf("at CREATE TABLE: can't have empty table")
	} else if p.query.Type == Create {
		intersection := intersectGeneric(p.query.TableConstruction.nullable, p.query.TableConstruction.notnullable)
		if len(intersection) > 0 {
			return fmt.Errorf("at CREATE TABLE: cannot have column be both nullable and non-nullable for columns: %v", intersection)
		}
	}
	return nil
}

func (p *parser) logError() {
	if p.err == nil {
		return
	}
	fmt.Println(p.sql)
	fmt.Println(strings.Repeat(" ", p.i) + "^")
	fmt.Println(p.err)
}
