package node

import (
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
	"github.com/VenkatGGG/ChronosDb/internal/systemtest"
)

const (
	DefaultPGWireUser     = systemtest.DefaultPGWireUser
	DefaultPGWirePassword = systemtest.DefaultPGWirePassword
)

type Config = systemtest.ProcessNodeConfig
type State = systemtest.ProcessNodeState
type Process = systemtest.ProcessNode

func New(cfg Config) (*Process, error) {
	return systemtest.NewProcessNode(cfg)
}

func DefaultCatalog() (*chronossql.Catalog, error) {
	return systemtest.DefaultCatalog()
}
