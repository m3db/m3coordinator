// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package iter

import (
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/ident"
)

// SeriesBlock is a placeholder until it is added to M3DB
type SeriesBlock struct {
	Start          time.Time
	End            time.Time
	SeriesIterator encoding.SeriesIterator
}

// SeriesBlocks is a placeholder until it is added to M3DB
type SeriesBlocks struct {
	ID     ident.ID
	Blocks []SeriesBlock
}

// MultiSeriesBlock represents a vertically oriented block
type MultiSeriesBlock struct {
	Start           time.Time
	End             time.Time
	SeriesIterators encoding.SeriesIterators
}

// StepIter iterates through a CompressedStepIterator vertically
type StepIter interface {
	Next() bool
	Current() Data
}

// SeriesIter iterates through a CompressedSeriesIterator horizontally
type SeriesIter interface {
	Next() bool
	Current() []ts.Datapoint // todo(braskin): optimize into a ts.Series later
}

// CompressedStepIterator implements the StepIter interface
type CompressedStepIterator struct {
	MultiSeriesBlock
}

// CompressedSeriesIterator implements the SeriesIter interface
type CompressedSeriesIterator struct {
	MultiSeriesBlock
}

// Data is the data per timestamp in a MultiSeriesBlock
type Data struct {
	timestamp time.Time
	values    []float64
}
