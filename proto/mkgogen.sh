#!/usr/bin/env bash

# Run this if hunter-proto is checked in the GOPATH.
# go get -d github.com/yancl/hunter-proto
# to check in the repo to the GOAPTH.
#
# To generate:
#
# cd $(go env GOPATH)/yancl/hunter-proto
# ./mkgogen.sh

OUTDIR="$(go env GOPATH)/src"

protoc --go_out=plugins=grpc:$OUTDIR dump/v1/dump.proto
