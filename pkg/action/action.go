package action

import (
	"context"
)

// Action is a single event handler
type Action struct {
	Name string
	Do   func(ctx context.Context, input map[string]interface{}) (output map[string]interface{}, err error)
}
