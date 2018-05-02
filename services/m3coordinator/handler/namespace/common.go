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

package namespace

import (
	"fmt"

	"github.com/m3db/m3cluster/kv"
	nsproto "github.com/m3db/m3db/generated/proto/namespace"
	"github.com/m3db/m3db/storage/namespace"
)

const (
	// M3DBNodeNamespacesKey is the KV key that holds namespaces
	M3DBNodeNamespacesKey = "m3db.node.namespaces"
)

// Handler represents a generic handler for namespace endpoints.
type Handler struct {
	store kv.Store
}

// Metadata returns the current metadata in the given store and its version
func Metadata(store kv.Store) ([]namespace.Metadata, int, error) {
	value, err := store.Get(M3DBNodeNamespacesKey)
	if err != nil {
		if err == kv.ErrNotFound {
			return []namespace.Metadata{}, -1, nil
		}

		return nil, -1, err
	}

	var protoRegistry nsproto.Registry
	if err := value.Unmarshal(&protoRegistry); err != nil {
		return nil, -1, fmt.Errorf("unable to parse value, err: %v", err)
	}

	nsMap, err := namespace.FromProto(protoRegistry)
	if err != nil {
		return nil, -1, err
	}

	return nsMap.Metadatas(), value.Version(), nil
}
