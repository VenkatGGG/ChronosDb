package replica

import (
	"encoding/json"
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/lease"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

// CommandType identifies the replicated command payload carried in a log entry.
type CommandType string

const (
	CommandTypePutValue         CommandType = "put_value"
	CommandTypeSetLease         CommandType = "set_lease"
	CommandTypeUpdateDescriptor CommandType = "update_descriptor"
	CommandTypeSplitRange       CommandType = "split_range"
)

// Command is a versioned replica state-machine command.
type Command struct {
	Version    uint8             `json:"version"`
	Type       CommandType       `json:"type"`
	Put        *PutValue         `json:"put,omitempty"`
	Lease      *SetLease         `json:"lease,omitempty"`
	Descriptor *UpdateDescriptor `json:"descriptor,omitempty"`
	Split      *SplitRange       `json:"split,omitempty"`
}

// PutValue writes one committed MVCC value.
type PutValue struct {
	LogicalKey []byte        `json:"logical_key"`
	Timestamp  hlc.Timestamp `json:"timestamp"`
	Value      []byte        `json:"value"`
}

// SetLease installs a new lease record.
type SetLease struct {
	Record lease.Record `json:"record"`
}

// UpdateDescriptor applies a generation-checked descriptor update.
type UpdateDescriptor struct {
	ExpectedGeneration uint64               `json:"expected_generation"`
	Descriptor         meta.RangeDescriptor `json:"descriptor"`
}

// SplitRange applies an atomic range split trigger.
type SplitRange struct {
	ExpectedGeneration uint64               `json:"expected_generation"`
	MetaLevel          meta.Level           `json:"meta_level"`
	Left               meta.RangeDescriptor `json:"left"`
	Right              meta.RangeDescriptor `json:"right"`
}

// Marshal encodes the command into its replicated binary form.
func (c Command) Marshal() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(c)
}

// UnmarshalCommand decodes a replicated command.
func UnmarshalCommand(data []byte) (Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return Command{}, fmt.Errorf("decode command: %w", err)
	}
	return cmd, cmd.Validate()
}

// Validate checks that the command payload matches the type tag.
func (c Command) Validate() error {
	if c.Version == 0 {
		c.Version = 1
	}
	if c.Version != 1 {
		return fmt.Errorf("command: unsupported version %d", c.Version)
	}
	switch c.Type {
	case CommandTypePutValue:
		if c.Put == nil || c.Lease != nil || c.Descriptor != nil || c.Split != nil {
			return fmt.Errorf("command: put_value payload mismatch")
		}
		if len(c.Put.LogicalKey) == 0 {
			return fmt.Errorf("command: logical key required")
		}
		if c.Put.Timestamp.IsZero() {
			return fmt.Errorf("command: put timestamp required")
		}
	case CommandTypeSetLease:
		if c.Lease == nil || c.Put != nil || c.Descriptor != nil || c.Split != nil {
			return fmt.Errorf("command: set_lease payload mismatch")
		}
		if err := c.Lease.Record.Validate(); err != nil {
			return err
		}
	case CommandTypeUpdateDescriptor:
		if c.Descriptor == nil || c.Put != nil || c.Lease != nil || c.Split != nil {
			return fmt.Errorf("command: update_descriptor payload mismatch")
		}
		if err := c.Descriptor.Descriptor.Validate(); err != nil {
			return err
		}
	case CommandTypeSplitRange:
		if c.Split == nil || c.Put != nil || c.Lease != nil || c.Descriptor != nil {
			return fmt.Errorf("command: split_range payload mismatch")
		}
		switch c.Split.MetaLevel {
		case meta.LevelMeta1, meta.LevelMeta2:
		default:
			return fmt.Errorf("command: split_range meta level %d is invalid", c.Split.MetaLevel)
		}
		if err := c.Split.Left.Validate(); err != nil {
			return err
		}
		if err := c.Split.Right.Validate(); err != nil {
			return err
		}
		if c.Split.Left.RangeID == c.Split.Right.RangeID {
			return fmt.Errorf("command: split_range must create a distinct right range")
		}
	default:
		return fmt.Errorf("command: unknown type %q", c.Type)
	}
	return nil
}
