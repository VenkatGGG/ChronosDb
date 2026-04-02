package sql

import vsqlparser "vitess.io/vitess/go/vt/sqlparser"

// Parser wraps the Vitess SQL parser for the ChronosDB SQL front door.
type Parser struct {
	parser *vsqlparser.Parser
}

// NewParser constructs the parser used by the SQL front door.
func NewParser() *Parser {
	return &Parser{parser: vsqlparser.NewTestParser()}
}

// Parse parses one SQL statement.
func (p *Parser) Parse(query string) (vsqlparser.Statement, error) {
	return p.parser.Parse(query)
}

// Parse2 parses one SQL statement and returns the discovered bind-variable set.
func (p *Parser) Parse2(query string) (vsqlparser.Statement, vsqlparser.BindVars, error) {
	return p.parser.Parse2(query)
}
