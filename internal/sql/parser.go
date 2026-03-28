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
