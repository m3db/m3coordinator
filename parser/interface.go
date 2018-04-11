package parser

import (
	"fmt"
)

// Parser consists of the language specific representation of AST and can convert into a common DAG
type Parser interface {
	DAG() (Transforms, Edges, error)
	String() string
}

// TransformID uniquely identifies all transforms in DAG
type TransformID string

// Operation is a function definition. It is immutable and contains no state
type Operation interface {
	fmt.Stringer
	OpType() string
	Node() OpNode
}

type OpNode interface {
}

// Transforms is a slice of Transform
type Transforms []Transform

// Transform represents an immutable node in the common DAG with a unique identifier.
// TODO: make this serializable
type Transform struct {
	ID TransformID
	Op Operation
}

// Node creates a transform node which works on functions and contains state
func (t Transform) Node() TransformNode {
	return NewTransformNode(t.Op.Node(), t)
}

func (t Transform) String() string {
	return fmt.Sprintf("ID: %s, Op: %s", t.ID, t.Op)
}

// Edge identifies parent-child relation between transforms
type Edge struct {
	ParentID TransformID
	ChildID  TransformID
}

func (e Edge) String() string {
	return fmt.Sprintf("parent: %s, child: %s", e.ParentID, e.ChildID)
}

// Edges is a slice of Edge
type Edges []Edge

// NewTransformFromOperation creates a new transform
func NewTransformFromOperation(Op Operation, nextID int) Transform {
	return Transform{
		Op: Op,
		ID: TransformID(fmt.Sprintf("%v", nextID)),
	}
}

// Transform node works on functions and contains state
type TransformNode struct {
	Op        OpNode
	transform Transform
}

func NewTransformNode(op OpNode, transform Transform) TransformNode {
	return TransformNode{
		Op:        op,
		transform: transform,
	}
}
