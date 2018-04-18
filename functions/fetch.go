package functions

import (
	"context"
	"fmt"
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/parser"
)

// FetchType gets the series from storage
const FetchType = "fetch"

// FetchOp stores required properties for fetch
type FetchOp struct {
	Name     string
	Range    time.Duration
	Offset   time.Duration
	Matchers models.Matchers
}

type FetchNode struct {
	op        FetchOp
	controller *parser.TransformController
}

// OpType for the operator
func (o FetchOp) OpType() string {
	return FetchType
}


// Execute
func (o FetchOp) Execute(ctx context.Context) error {
	return nil
}

// String representation
func (o FetchOp) String() string {
	return fmt.Sprintf("type: %s. name: %s, range: %v, offset: %v, matchers: %v", o.OpType(), o.Name, o.Range, o.Offset, o.Matchers)
}

// Node for the operator
func (o FetchOp) Node(controller *parser.TransformController) parser.OpNode {
	return &FetchNode{op: o, controller: controller}
}

func (n *FetchNode) Execute(ctx context.Context) error {
	return nil
}