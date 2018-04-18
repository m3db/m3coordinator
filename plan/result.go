package plan

import (
	"fmt"

	"github.com/m3db/m3coordinator/parser"
)

// ResultType gets the results
const ResultType = "result"

// ResultOp is resonsible for delivering results to the clients
type ResultOp struct {
}

// OpType is the type of operation
func (r ResultOp) OpType() string {
	return ResultType
}

// String representation
func (r ResultOp) String() string {
	return fmt.Sprintf("type: %s", r.OpType())
}

// ResultNode is used to provide the results to the caller from the query execution
type ResultNode struct {

}

// Node returns the execution node
func (r ResultOp) Node(controller *parser.TransformController) parser.OpNode {
	return &ResultNode{}
}
