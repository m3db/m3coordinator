#!/usr/bin/env bash
#
# Generate all protobuf bindings.
# Run from repository root.
DIR=$1
pushd ${DIR}
protoc --gofast_out=plugins=grpc:. --proto_path=.:$GOPATH/src/github.com/m3db/m3coordinator/vendor *.proto
popd

# //HERE change this location to one higher directory and modify as necessary