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

// Operation is a function that can be applied to data
type Operation interface {
	OpType() string
	String() string
}

// Transforms is a slice of Transform
type Transforms []*Transform

// Transform is a node in common DAG which can be uniquely identified. It is immutable
type Transform struct {
	id TransformID
	op Operation
}

// ID is a unique ID for the transform
func (t *Transform) ID() TransformID {
	return t.id
}

// Op is a operation for the transform
func (t *Transform) Op() Operation {
	return t.op
}

func (t *Transform) String() string {
	return fmt.Sprintf("ID: %s, Op: %s", t.ID(), t.Op())
}

// Edge identifies parent-child relation between transforms
type Edge struct {
	ParentID TransformID
	ChildID  TransformID
}

func (e *Edge) String() string {
	return fmt.Sprintf("parent: %s, child: %s", e.ParentID, e.ChildID)
}

// Edges is a slice of Edge
type Edges []*Edge

// NewTransformFromOperation creates a new transform
func NewTransformFromOperation(Op Operation, nextID int) *Transform {
	return &Transform{
		op: Op,
		id: TransformID(fmt.Sprintf("%v", nextID)),
	}
}
