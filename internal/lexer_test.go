package internal

import (
	"testing"

	"github.com/treeform-system/rootdb/internal/token"
)

// TODO: Update for different statements later on
func TestNextToken(t *testing.T) {
	input := `CREATE TABLE "_sometable@#$%" (column1 int Primary Key, column30 bool, somecolumn char(10), column400 float, column5 int);
	.72 11.7 11.7.8 90 36.7.7.7.;
	INSERT INTO "MyTable10" (column1,column5) VALUES ('1','somecharss', 'true','1.23'),
	(2,'10letters', false,	.69,4.69);
	Select * FROM "table" WHERE col1 = true AND "col^&ψumn1" <= 1 AND column5 > 2;
	"MyTable10" WHERE column5 >= column1;
	. ! != Set AS
	`
	tests := []struct {
		expectedType    token.TokenType
		expectedLiteral string
	}{
		{token.CREATE, "CREATE"},
		{token.TABLE, "TABLE"},
		{token.IDENT, "_sometable@#$%"},
		{token.LPAREN, "("},
		{token.IDENT, "column1"},
		{token.INT, "int"},
		{token.PRIMARY, "Primary"},
		{token.KEY, "Key"},
		{token.COMMA, ","},
		{token.IDENT, "column30"},
		{token.BOOL, "bool"},
		{token.COMMA, ","},
		{token.IDENT, "somecolumn"},
		{token.CHAR, "char"},
		{token.LPAREN, "("},
		{token.NUMBERLITERAL, "10"},
		{token.RPAREN, ")"},
		{token.COMMA, ","},
		{token.IDENT, "column400"},
		{token.FLOAT, "float"},
		{token.COMMA, ","},
		{token.IDENT, "column5"},
		{token.INT, "int"},
		{token.RPAREN, ")"},
		{token.SEMICOLON, ";"},
		{token.NUMBERLITERAL, ".72"},
		{token.NUMBERLITERAL, "11.7"},
		{token.ILLEGAL, ""},
		{token.NUMBERLITERAL, "90"},
		{token.ILLEGAL, ""},
		{token.SEMICOLON, ";"},
		{token.INSERT, "INSERT"},
		{token.INTO, "INTO"},
		{token.IDENT, "MyTable10"},
		{token.LPAREN, "("},
		{token.IDENT, "column1"},
		{token.COMMA, ","}, {token.IDENT, "column5"},
		{token.RPAREN, ")"},
		{token.VALUES, "VALUES"},
		{token.LPAREN, "("},
		{token.STRINGLITERAL, "1"},
		{token.COMMA, ","},
		{token.STRINGLITERAL, "somecharss"},
		{token.COMMA, ","},
		{token.STRINGLITERAL, "true"},
		{token.COMMA, ","},
		{token.STRINGLITERAL, "1.23"},
		{token.RPAREN, ")"},
		{token.COMMA, ","},
		{token.LPAREN, "("},
		{token.NUMBERLITERAL, "2"},
		{token.COMMA, ","},
		{token.STRINGLITERAL, "10letters"},
		{token.COMMA, ","},
		{token.FALSE, "false"},
		{token.COMMA, ","},
		{token.NUMBERLITERAL, ".69"},
		{token.COMMA, ","},
		{token.NUMBERLITERAL, "4.69"},
		{token.RPAREN, ")"},
		{token.SEMICOLON, ";"},
		{token.SELECT, "Select"},
		{token.ASTERISK, "*"},
		{token.FROM, "FROM"},
		{token.IDENT, "table"},
		{token.WHERE, "WHERE"},
		{token.IDENT, "col1"},
		{token.EQ, "="},
		{token.TRUE, "true"},
		{token.AND, "AND"},
		{token.IDENT, "col^&ψumn1"},
		{token.LTE, "<="},
		{token.NUMBERLITERAL, "1"},
		{token.AND, "AND"},
		{token.IDENT, "column5"},
		{token.GT, ">"},
		{token.NUMBERLITERAL, "2"},
		{token.SEMICOLON, ";"},
		{token.IDENT, "MyTable10"},
		{token.WHERE, "WHERE"},
		{token.IDENT, "column5"},
		{token.GTE, ">="},
		{token.IDENT, "column1"},
		{token.SEMICOLON, ";"},
		{token.PERIOD, "."},
		{token.BANG, "!"},
		{token.NOT_EQ, "!="},
		{token.SET, "Set"},
		{token.AS, "AS"},
		{token.EOF, ""},
	}

	l := NewLexer(input)

	for i, tt := range tests {
		tok := l.NextToken()

		//t.Logf("tests[%d] - tokentype current. got =%q - (%s) ", i, tok.Type, tok.Literal)
		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%q, got =%q", i, tt.expectedType, tok.Type)
		}
		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q", i, tt.expectedLiteral, tok.Literal)
		}
	}
}
