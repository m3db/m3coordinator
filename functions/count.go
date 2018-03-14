package functions

// CountType counts number of elements in the vector
const CountType = "count"

// CountOp stores required properties for count
type CountOp struct {
}

// Type for the operator
func (o *CountOp) Type() string {
	return CountType
}
