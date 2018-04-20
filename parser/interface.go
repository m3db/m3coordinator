package parser

import (
	"context"
	"fmt"

	"github.com/m3db/m3coordinator/storage"
)

// Parser consists of the language specific representation of AST and can convert into a common DAG
type Parser interface {
	DAG() (Nodes, Edges, error)
	String() string
}

// NodeID uniquely identifies all transforms in DAG
type NodeID string

// Params is a function definition. It is immutable and contains no state
type Params interface {
	fmt.Stringer
	OpType() string
}

// Source represents data sources which are handled differently than other transforms as they are always independent and can always be parallelized
type Source interface {
	Execute(ctx context.Context) error
}

type TransformParams interface {
	Params
	Node(controller *TransformController) OpNode
}

type SourceParams interface {
	Params
	Node(controller *TransformController, storage storage.Storage) Source
}

// OpNode represents the execution node
type OpNode interface {
}

// Nodes is a slice of Node
type Nodes []Node

// Node represents an immutable node in the common DAG with a unique identifier.
// TODO: make this serializable
type Node struct {
	ID NodeID
	Op Params
}

func CreateSource(ID NodeID, params SourceParams, storage storage.Storage) (Source, *TransformController) {
	controller := &TransformController{id: ID}
	return params.Node(controller, storage), controller
}

// CreateTransform creates a transform node which works on functions and contains state
func CreateTransform(ID NodeID, params TransformParams) (OpNode, *TransformController) {
	controller := &TransformController{id: ID}
	return params.Node(controller), controller
}

func (t Node) String() string {
	return fmt.Sprintf("ID: %s, Op: %s", t.ID, t.Op)
}

// Edge identifies parent-child relation between transforms
type Edge struct {
	ParentID NodeID
	ChildID  NodeID
}

func (e Edge) String() string {
	return fmt.Sprintf("parent: %s, child: %s", e.ParentID, e.ChildID)
}

// Edges is a slice of Edge
type Edges []Edge

// NewTransformFromOperation creates a new transform
func NewTransformFromOperation(Op Params, nextID int) Node {
	return Node{
		Op: Op,
		ID: NodeID(fmt.Sprintf("%v", nextID)),
	}
}

// TransformController controls the caching and forwarding the request to downstream.
type TransformController struct {
	id         NodeID
	transforms []OpNode
}

// AddTransform adds a dependent transformation to the controller
func (t *TransformController) AddTransform(node OpNode) {
	t.transforms = append(t.transforms, node)
}
