package sql

import vsqlparser "vitess.io/vitess/go/vt/sqlparser"

// Parser exposes the Vitess parser behind the ChronosDB SQL front door.
type Parser struct {
	parser *vsqlparser.Parser
}

// NewParser returns a parser configured for the SQL front door.
func NewParser() *Parser {
	return &Parser{parser: vsqlparser.NewTestParser()}
}

// Parse delegates one statement parse to Vitess.
func (p *Parser) Parse(query string) (vsqlparser.Statement, error) {
	return p.parser.Parse(query)
}

// Parse2 also returns the discovered bind-variable set.
func (p *Parser) Parse2(query string) (vsqlparser.Statement, vsqlparser.BindVars, error) {
	return p.parser.Parse2(query)
}
