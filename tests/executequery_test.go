// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !emulator
// +build !emulator

package tests

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
)

func toGoMap(p *btpb.Value) map[*btpb.Value]*btpb.Value {
	var m = make(map[*btpb.Value]*btpb.Value)
	if p.GetKind() == nil {
		return nil
	} else {
		arr := p.GetArrayValue()
		for _, kvArr := range arr.Values {
			m[kvArr.GetArrayValue().Values[0]] = kvArr.GetArrayValue().Values[1]
		}
	}
	return m
}

// We don't use cmp directly because we need special handling for maps to ignore ordering.
// This is because maps are represented as Array values (which are ordered) but clients will
// not preserve that ordering when they convert to native maps.
// Note that this will not currently handle nested map ordering correctly if we ever support
// nested maps
func assertRowEqual(t *testing.T, want *testproxypb.SqlRow, got *testproxypb.SqlRow, metadata *testproxypb.ResultSetMetadata) {
	var equal = len(metadata.Columns) == len(want.Values) && len(want.Values) == len(got.Values)
	if !equal {
		println("Lengths do not match. Full diff (which may be incorrectly show diffs based on map ordering):")
		println(cmp.Diff(want, got, protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001)))
	}
	for i := 0; i < len(metadata.Columns); i++ {
		col := metadata.Columns[i]
		var diff = ""
		switch col.Type.GetKind().(type) {
		case *btpb.Type_MapType:
			// We don't want to enforce map entry order, but we still want to make sure key-value ordering is correct
			// and that ordering of nested fields is correct. So this first converts to a go map and then compares those maps.
			// In order to use protoCmp on a go map with proto messages as keys the recommended approach is to convert it back
			// to a slice using SortMaps, and then use protocmp.Transform. Transform does not work on map keys
			diff = cmp.Diff(toGoMap(want.Values[i]), toGoMap(got.Values[i]), cmpopts.SortMaps(func(x, y *btpb.Value) bool {
				// We only care that the order is consistent here.
				return x.String() < y.String()
			}), protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001))
			break
		default:
			diff = cmp.Diff(want.Values[i], got.Values[i], protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001))
			break
		}
		if diff != "" {
			equal = false
			fmt.Println("Value at column ", i, " does not match. Diff:")
			println(diff)
		}
	}
	assert.True(t, equal)
}

func TestExecuteQuery_EmptyResponse(t *testing.T) {
	recorder := make(chan *executeQueryReqRecord, 1)
	server := initMockServer(t)
	server.ExecuteQueryFn = mockExecuteQueryFn(recorder, &executeQueryAction{
		response:    md(column("test", strType())),
		endOfStream: true,
	})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(column("test", strType())), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 0)

	origReq := <-recorder
	if diff := cmp.Diff(req.Request, origReq.req, protocmp.Transform(), protocmp.IgnoreEmptyMessages()); diff != "" {
		t.Errorf("diff found (-want +got):\n%s", diff)
	}
}

func TestExecuteQuery_SingleSimpleRow(t *testing.T) {
	server := initMockServer(t)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    md(column("test", strType())),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", strVal("foo")),
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(column("test", strType())), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(strVal("foo")), res.Rows[0], res.Metadata)
}

func TestExecuteQuery_TypesTest(t *testing.T) {
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("strCol", strType()),
		column("bytesCol", bytesType()),
		column("int64Col", int64Type()),
		column("boolCol", boolType()),
		column("float32Col", float32Type()),
		column("float64Col", float64Type()),
		column("dateCol", dateType()),
		column("tsCol", timestampType()),
		column("structCol", structType(
			structField("strField", strType()),
			structField("intField", int64Type()),
			structField("arrField", arrayType(bytesType())))),
		column("arrayCol", arrayType(bytesType())),
		// simple map
		column("mapCol", mapType(bytesType(), strType())),
		// historical map
		column("historicalMap", mapType(
			bytesType(),
			arrayType(structType(
				structField("timestamp", timestampType()),
				structField("value", bytesType()))))),
	}
	expectedValues := []*btpb.Value{
		strVal("strVal"),
		bytesVal([]byte("bytesVal")),
		intVal(10),
		boolVal(true),
		floatVal(1.2),
		floatVal(1.3),
		dateVal(2024, 9, 1),
		timestampVal(2000, 1000),
		structVal(strVal("field"), intVal(100), arrayVal(bytesVal([]byte("foo")), bytesVal([]byte("bar")))),
		arrayVal(bytesVal([]byte("elem"))),
		mapVal(mapEntry(bytesVal([]byte("key")), strVal("val"))),
		mapVal(mapEntry(bytesVal([]byte("key")), arrayVal(
			structVal(timestampVal(10000, 1000), bytesVal([]byte("val1"))),
			structVal(timestampVal(20000, 1000), bytesVal([]byte("val2")))))),
	}
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    md(columns...),
			endOfStream: false,
		},
		&executeQueryAction{
			response: partialResultSet(
				"token",
				expectedValues...,
			),
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 12)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(expectedValues...), res.Rows[0], res.Metadata)
}

func TestExecuteQuery_NullsTest(t *testing.T) {
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("strCol", strType()),
		column("bytesCol", bytesType()),
		column("int64Col", int64Type()),
		column("boolCol", boolType()),
		column("float32Col", float32Type()),
		column("float64Col", float64Type()),
		column("dateCol", dateType()),
		column("tsCol", timestampType()),
		column("structCol", structType(
			structField("strField", strType()),
			structField("intField", int64Type()),
			structField("arrField", arrayType(bytesType())))),
		column("arrayCol", arrayType(bytesType())),
		// simple map
		column("mapCol", mapType(bytesType(), strType())),
		// historical map
		column("historicalMap", mapType(
			bytesType(),
			arrayType(structType(
				structField("timestamp", timestampType()),
				structField("value", bytesType()))))),
	}
	expectedValues := []*btpb.Value{
		nullVal(), nullVal(), nullVal(), nullVal(), nullVal(), nullVal(),
		nullVal(), nullVal(), nullVal(), nullVal(), nullVal(), nullVal(),
	}
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    md(columns...),
			endOfStream: false,
		},
		&executeQueryAction{
			response: partialResultSet(
				"token",
				expectedValues...,
			),
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 12)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(expectedValues...), res.Rows[0], res.Metadata)
}

func TestExecuteQuery_NestedNullsTest(t *testing.T) {
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("structCol", structType(
			structField("strField", strType()),
			structField("intField", int64Type()),
			structField("arrField", arrayType(bytesType())))),
		column("arrayCol", arrayType(int64Type())),
		// simple map
		column("mapCol", mapType(bytesType(), strType())),
	}
	expectedValues := []*btpb.Value{
		structVal(strVal("foo"), nullVal(), arrayVal(bytesVal([]byte("foo")), nullVal())),
		arrayVal(intVal(100), nullVal(), intVal(200), nullVal()),
		mapVal(mapEntry(nullVal(), strVal("foo")), mapEntry(bytesVal([]byte("bar")), nullVal())),
	}
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    md(columns...),
			endOfStream: false,
		},
		&executeQueryAction{
			response: partialResultSet(
				"token",
				expectedValues...,
			),
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 3)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(expectedValues...), res.Rows[0], res.Metadata)
}

func TestExecuteQuery_MapAllowsDuplicateKey(t *testing.T) {
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("mapCol", mapType(bytesType(), strType())),
	}
	expectedValues := []*btpb.Value{
		mapVal(mapEntry(bytesVal([]byte("foo")), strVal("foo")), mapEntry(bytesVal([]byte("foo")), strVal("bar"))),
	}
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    md(columns...),
			endOfStream: false,
		},
		&executeQueryAction{
			response: partialResultSet(
				"token",
				expectedValues...,
			),
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	// For values with duplicate keys, the last value should win.
	assertRowEqual(t, testProxyRow(mapVal(mapEntry(bytesVal([]byte("foo")), strVal("bar")))), res.Rows[0], res.Metadata)
}

func TestExecuteQuery_QueryParams(t *testing.T) {
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("strCol", strType()),
		column("bytesCol", bytesType()),
		column("intCol", int64Type()),
		column("doubleCol", float64Type()),
		column("floatCol", float32Type()),
		column("boolCol", boolType()),
		column("tsCol", timestampType()),
		column("dateCol", dateType()),
		column("byteArrayCol", arrayType(bytesType())),
		column("stringArrayCol", arrayType(strType())),
		column("intArrayCol", arrayType(int64Type())),
		column("floatArrayCol", arrayType(float32Type())),
		column("doubleArrayCol", arrayType(float64Type())),
		column("boolArrayCol", arrayType(boolType())),
		column("tsArrayCol", arrayType(timestampType())),
		column("dateArrayParam", arrayType(dateType())),
	}
	expectedValues := []*btpb.Value{
		strVal("s"),
		bytesVal([]byte("b")),
		intVal(200),
		floatVal(1.55),
		floatVal(1.4),
		boolVal(true),
		timestampVal(1000000, 1000),
		dateVal(2024, 9, 1),
		arrayVal(bytesVal([]byte("foo")), nullVal()),
		arrayVal(strVal("foo"), nullVal()),
		arrayVal(intVal(123), nullVal()),
		arrayVal(floatVal(1.23), nullVal()),
		arrayVal(floatVal(4.56), nullVal()),
		arrayVal(boolVal(true), nullVal()),
		arrayVal(timestampVal(100000000, 2000), nullVal()),
		arrayVal(dateVal(2024, 9, 2), nullVal()),
	}

	recorder := make(chan *executeQueryReqRecord, 1)
	server.ExecuteQueryFn = mockExecuteQueryFn(recorder,
		&executeQueryAction{
			response:    md(columns...),
			endOfStream: false,
		},
		&executeQueryAction{
			response: partialResultSet(
				"token",
				expectedValues...,
			),
			endOfStream: true,
		})

	params := map[string]*btpb.Value{
		"stringParam":      strValWithType("s"),
		"bytesParam":       bytesValWithType([]byte("b")),
		"int64Param":       intValWithType(200),
		"doubleParam":      float64ValWithType(1.55),
		"floatParam":       float32ValWithType(1.4),
		"boolParam":        boolValWithType(true),
		"tsParam":          timestampValWithType(1000000, 1000),
		"dateParam":        dateValWithType(2024, 9, 1),
		"byteArrayParam":   arrayValWithType(bytesType(), bytesVal([]byte("foo")), nullVal()),
		"stringArrayParam": arrayValWithType(strType(), strVal("foo"), nullVal()),
		"intArrayParam":    arrayValWithType(int64Type(), intVal(123), nullVal()),
		"floatArrayParam":  arrayValWithType(float32Type(), floatVal(1.23), nullVal()),
		"doubleArrayParam": arrayValWithType(float64Type(), floatVal(4.56), nullVal()),
		"boolArrayParam":   arrayValWithType(boolType(), boolVal(true), nullVal()),
		"tsArrayParam":     arrayValWithType(timestampType(), timestampVal(100000000, 2000), nullVal()),
		"dateArrayParam":   arrayValWithType(dateType(), dateVal(2024, 9, 2), nullVal()),
	}
	realReq := &btpb.ExecuteQueryRequest{
		InstanceName: instanceName,
		Query: `SELECT @stringParam AS strCol, @bytesParam as bytesCol, @int64Param AS intCol, @doubleParam AS doubleCol, 
				@floatParam AS floatCol, @boolParam AS boolCol, @tsParam AS tsCol, @dateParam AS dateCol, 
				@byteArrayParam AS byteArrayCol, @stringArrayParam AS stringArrayCol, @intArrayParam AS intArrayCol, 
				@floatArrayParam AS floatArrayCol, @doubleArrayParam AS doubleArrayCol, @boolArrayParam AS boolArrayCol, 
				@tsArrayParam AS tsArrayCol, @dateArrayParam AS dateArrayCol`,
		Params: params,
	}
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request:  realReq,
	}

	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	loggedReq := <-recorder
	assert.True(t, cmp.Equal(loggedReq.req, realReq, protocmp.Transform(), cmpopts.EquateApprox(0, 0.0001)))
	assert.Equal(t, len(res.Metadata.Columns), 16)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(expectedValues...), res.Rows[0], res.Metadata)
}

func TestExecuteQuery_MaxTimestampTest(t *testing.T) {
	// Timestamp can be in micros up to max long
	var maxTsSeconds int64 = math.MaxInt64 / 1000 / 1000
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("maxTsCol", timestampType()),
	}
	expectedValues := []*btpb.Value{
		timestampVal(maxTsSeconds, 0),
	}
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    md(columns...),
			endOfStream: false,
		},
		&executeQueryAction{
			response: partialResultSet(
				"token",
				expectedValues...,
			),
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(expectedValues...), res.Rows[0], res.Metadata)
}

func TestExecuteQuery_ChunkingTest(t *testing.T) {
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("bytesCol", bytesType()),
		column("arrayCol", arrayType(bytesType())),
		// historical map
		column("historicalMap", mapType(
			bytesType(),
			arrayType(structType(
				structField("timestamp", timestampType()),
				structField("value", bytesType()))))),
	}
	expectedValues := []*btpb.Value{
		// row 1
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
		// row 2
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
	}
	results := chunkedPartialResultSet(3, "token", expectedValues...)
	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    md(columns...),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    results[0],
			endOfStream: false,
		},
		&executeQueryAction{
			response:    results[1],
			endOfStream: false,
		},
		&executeQueryAction{
			response:    results[2],
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 3)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 2)
	assertRowEqual(t, testProxyRow(expectedValues[0:3]...), res.Rows[0], res.Metadata)
	assertRowEqual(t, testProxyRow(expectedValues[3:6]...), res.Rows[1], res.Metadata)
}

func TestExecuteQuery_BatchesTest(t *testing.T) {
	server := initMockServer(t)
	columns := []*btpb.ColumnMetadata{
		column("bytesCol", bytesType()),
		column("arrayCol", arrayType(bytesType())),
		// historical map
		column("historicalMap", mapType(
			bytesType(),
			arrayType(structType(
				structField("timestamp", timestampType()),
				structField("value", bytesType()))))),
	}
	expectedValues := []*btpb.Value{
		// row 1
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
		// row 2
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
		// row 3
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
		// row 4
		bytesVal(generateBytes(512)),
		arrayVal(bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128)), bytesVal(generateBytes(128))),
		mapVal(mapEntry(bytesVal(generateBytes(10)), arrayVal(structVal(timestampVal(0, 0), bytesVal(generateBytes(512)))))),
	}
	batch1 := chunkedPartialResultSet(2, "token1", expectedValues[0:6]...)
	batch2 := chunkedPartialResultSet(2, "token2", expectedValues[6:12]...)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    md(columns...),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    batch1[0],
			endOfStream: false,
		},
		&executeQueryAction{
			response:    batch1[1],
			endOfStream: false,
		},
		&executeQueryAction{
			response:    batch2[0],
			endOfStream: false,
		},
		&executeQueryAction{
			response:    batch2[1],
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 3)
	assert.True(t, cmp.Equal(res.Metadata, testProxyMd(columns...), protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 4)
	assertRowEqual(t, testProxyRow(expectedValues[0:3]...), res.Rows[0], res.Metadata)
	assertRowEqual(t, testProxyRow(expectedValues[3:6]...), res.Rows[1], res.Metadata)
	assertRowEqual(t, testProxyRow(expectedValues[6:9]...), res.Rows[2], res.Metadata)
	assertRowEqual(t, testProxyRow(expectedValues[9:12]...), res.Rows[3], res.Metadata)
}

func TestExecuteQuery_FailsOnMissingMetadata(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: partialResultSet(
				"token",
				strVal("foo"),
			),
			endOfStream: false,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assert.Equal(t, "First response must always contain metadata", res.GetStatus().GetMessage())
}

func TestExecuteQuery_FailsOnEmptyMetadata(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    md(),
			endOfStream: false,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assert.Equal(t, "columns cannot be empty", res.GetStatus().GetMessage())
}

func TestExecuteQuery_FailsOnDuplicateMetadata(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response:    md(column("bytesCol", bytesType())),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    md(column("bytesCol", bytesType())),
			endOfStream: false,
		},
	)
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assert.Equal(t, "Expected results response, but received: METADATA", res.GetStatus().GetMessage())
}

func TestExecuteQuery_FailsOnInvalidType(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: md(
				column("test", bytesType()),
				column("invalid", &btpb.Type{}),
			),
			endOfStream: false,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assert.Equal(t, "Column type cannot be empty", res.GetStatus().GetMessage())
}

func TestExecuteQuery_FailsOnNotEnoughData(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: md(
				column("foo", bytesType()),
				column("bar", strType()),
			),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", bytesVal([]byte("foo"))),
			endOfStream: false,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assert.True(t, strings.Contains(res.GetStatus().GetMessage(), "Incomplete row received"))
}

func TestExecuteQuery_FailsOnNotEnoughDataWithCompleteRows(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: md(
				column("foo", bytesType()),
				column("bar", strType()),
			),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", bytesVal([]byte("foo")), strVal("s"), bytesVal([]byte("bar"))),
			endOfStream: false,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assert.True(t, strings.Contains(res.GetStatus().GetMessage(), "Incomplete row received"))
}

func TestExecuteQuery_FailsOnTypeMismatch(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: md(
				column("foo", bytesType()),
				column("bar", strType()),
			),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", bytesVal([]byte("foo")), strVal("s"), bytesVal([]byte("bar")), intVal(42)),
			endOfStream: false,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assert.Equal(t, "Value kind must be STRING_VALUE for columns of type: STRING", res.GetStatus().GetMessage())
}

func TestExecuteQuery_FailsOnTypeMismatchWithinMap(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: md(
				column("foo", mapType(strType(), int64Type())),
			),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", mapVal(mapEntry(strVal("s"), strVal("wrong")))),
			endOfStream: false,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assert.Equal(t, "Value kind must be INT_VALUE for columns of type: INT64", res.GetStatus().GetMessage())
}

func TestExecuteQuery_FailsOnTypeMismatchWithinArray(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: md(
				column("arr", arrayType(strType())),
			),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", arrayVal(strVal("foo"), intVal(1))),
			endOfStream: false,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assert.Equal(t, "Value kind must be STRING_VALUE for columns of type: STRING", res.GetStatus().GetMessage())
}

func TestExecuteQuery_FailsOnTypeMismatchWithinStruct(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: md(
				column("struct", structType(structField("s", strType()), structField("i", int64Type()))),
			),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", structVal(strVal("foo"), strVal("bar"))),
			endOfStream: false,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assert.Equal(t, "Value kind must be INT_VALUE for columns of type: INT64", res.GetStatus().GetMessage())
}

func TestExecuteQuery_FailsOnStructMissingField(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: md(
				column("struct", structType(structField("s", strType()), structField("i", int64Type()))),
			),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", structVal(strVal("foo"))),
			endOfStream: false,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
	assert.Equal(t, "Index 1 out of bounds for length 1", res.GetStatus().GetMessage())
}

func TestExecuteQuery_StructWithNoColumnNames(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: md(
				column("struct", structType(namelessStructField(strType()), namelessStructField(int64Type()))),
			),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", structVal(strVal("foo"), intVal(100)), structVal(strVal("bar"), intVal(101))),
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata,
		testProxyMd(column("struct",
			structType(namelessStructField(strType()), namelessStructField(int64Type())))),
		protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 2)
	assertRowEqual(t, testProxyRow(structVal(strVal("foo"), intVal(100))), res.Rows[0], res.Metadata)
	assertRowEqual(t, testProxyRow(structVal(strVal("bar"), intVal(101))), res.Rows[1], res.Metadata)
}

func TestExecuteQuery_StructWithDuplicateColumnNames(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: md(
				column("struct", structType(structField("foo", strType()), structField("foo", int64Type()))),
			),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", structVal(strVal("foo"), intVal(100)), structVal(strVal("bar"), intVal(101))),
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 1)
	assert.True(t, cmp.Equal(res.Metadata,
		testProxyMd(column("struct",
			structType(structField("foo", strType()), structField("foo", int64Type())))),
		protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 2)
	assertRowEqual(t, testProxyRow(structVal(strVal("foo"), intVal(100))), res.Rows[0], res.Metadata)
	assertRowEqual(t, testProxyRow(structVal(strVal("bar"), intVal(101))), res.Rows[1], res.Metadata)
}

func TestExecuteQuery_FailsOnSuccesfulStreamWithNoToken(t *testing.T) {
	server := initMockServer(t)

	server.ExecuteQueryFn = mockExecuteQueryFn(nil,
		&executeQueryAction{
			response: md(
				column("intCol", int64Type()),
				column("strCol", strType()),
			),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("", intVal(100), strVal("foo")),
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	res := doExecuteQueryOp(t, server, &req, nil)
	assert.Equal(t, int32(codes.Internal), res.GetStatus().GetCode())
}

func TestExecuteQuery_HeadersAreSet(t *testing.T) {
	server := initMockServer(t)

	recorder := make(chan *executeQueryReqRecord, 1)
	mdRecorder := make(chan metadata.MD, 1)
	server.ExecuteQueryFn = mockExecuteQueryFnWithMetadataSimple(recorder, mdRecorder,
		&executeQueryAction{
			response: md(
				column("foo", strType()),
				column("bar", int64Type()),
			),
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", strVal("foo"), intVal(100)),
			endOfStream: true,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	appProfileId := "headers-test"
	opts := clientOpts{
		profile: appProfileId,
	}
	res := doExecuteQueryOp(t, server, &req, &opts)
	checkResultOkStatus(t, res)
	assert.Equal(t, len(res.Metadata.Columns), 2)
	assert.True(t, cmp.Equal(res.Metadata,
		testProxyMd(column("foo", strType()),
			column("bar", int64Type())),
		protocmp.Transform()))
	assert.Equal(t, len(res.Rows), 1)
	assertRowEqual(t, testProxyRow(strVal("foo"), intVal(100)), res.Rows[0], res.Metadata)

	// Check the request headers in the metadata
	headers := <-mdRecorder
	if len(headers["user-agent"]) == 0 && len(headers["x-goog-api-client"]) == 0 {
		assert.Fail(t, "Client info is missing in the request header")
	}

	resource := headers["x-goog-request-params"][0]
	if !strings.Contains(resource, instanceName) && !strings.Contains(resource, url.QueryEscape(instanceName)) {
		assert.Fail(t, "Resource info is missing in the request header")
	}
	assert.Contains(t, resource, appProfileId)
}

func TestExecuteQuery_RespectsDeadline(t *testing.T) {
	server := initMockServer(t)
	recorder := make(chan *executeQueryReqRecord, 1)
	server.ExecuteQueryFn = mockExecuteQueryFn(recorder,
		&executeQueryAction{
			response:    md(column("test", strType())),
			delayStr:    "10s",
			endOfStream: false,
		})
	req := testproxypb.ExecuteQueryRequest{
		ClientId: t.Name(),
		Request: &btpb.ExecuteQueryRequest{
			InstanceName: instanceName,
			Query:        "SELECT * FROM table",
		},
	}
	opts := &clientOpts{
		timeout: &durationpb.Duration{Seconds: 2},
	}
	res := doExecuteQueryOp(t, server, &req, opts)
	// Check the runtime
	curTs := time.Now()
	loggedReq := <-recorder
	runTimeSecs := int(curTs.Unix() - loggedReq.ts.Unix())
	assert.GreaterOrEqual(t, runTimeSecs, 2)
	assert.Less(t, runTimeSecs, 8) // 8s (< 10s of server delay time) indicates timeout takes effect.

	// Check the DeadlineExceeded error. Some clients wrap the error code in the message,
	// so check the message if error code is not right.
	if res.GetStatus().GetCode() != int32(codes.DeadlineExceeded) {
		msg := res.GetStatus().GetMessage()
		assert.Contains(t, strings.ToLower(strings.ReplaceAll(msg, " ", "")), "deadlineexceeded")
	}
}

func TestExecuteQuery_ConcurrentRequests(t *testing.T) {
	concurrency := 5

	recorder := make(chan *executeQueryReqRecord, concurrency)
	actionSequences := [][]*executeQueryAction{
		[]*executeQueryAction{
			&executeQueryAction{
				response:    md(column("strCol", strType())),
				delayStr:    "2s",
				endOfStream: false,
			},
			&executeQueryAction{
				response:    partialResultSet("token", strVal("foo"), strVal("bar"), strVal("baz")),
				endOfStream: true,
			},
		},
		[]*executeQueryAction{
			&executeQueryAction{
				response:    md(column("intCol", int64Type()), column("boolCol", boolType())),
				delayStr:    "2s",
				endOfStream: false,
			},
			&executeQueryAction{
				response:    partialResultSet("token", intVal(1), boolVal(true)),
				delayStr:    "2s",
				endOfStream: false,
			},
			&executeQueryAction{
				response:    partialResultSet("token", intVal(2), boolVal(false)),
				delayStr:    "2s",
				endOfStream: true,
			},
		},
		[]*executeQueryAction{
			&executeQueryAction{
				response:    md(column("mapCol", mapType(strType(), strType())), column("strCol", strType())),
				delayStr:    "2s",
				endOfStream: false,
			},
			&executeQueryAction{
				response:    partialResultSet("token", mapVal(mapEntry(strVal("k"), strVal("v"))), strVal("foo")),
				delayStr:    "2s",
				endOfStream: true,
			},
		},
		[]*executeQueryAction{
			&executeQueryAction{
				response:    md(column("strCol", strType()), column("bytesCol", bytesType())),
				delayStr:    "2s",
				endOfStream: true,
			},
		},
		[]*executeQueryAction{
			&executeQueryAction{
				response:    md(column("arrayOfString", arrayType(strType()))),
				delayStr:    "2s",
				endOfStream: false,
			},
			&executeQueryAction{
				response:    partialResultSet("token", arrayVal(strVal("e1"), strVal("e2")), arrayVal(strVal("f1"), strVal("f2"))),
				delayStr:    "2s",
				endOfStream: true,
			},
		},
	}

	server := initMockServer(t)
	server.ExecuteQueryFn = mockExecuteQueryFnWithMetadata(recorder, nil, actionSequences)

	reqs := make([]*testproxypb.ExecuteQueryRequest, concurrency)
	for i := 0; i < concurrency; i++ {
		reqs[i] = &testproxypb.ExecuteQueryRequest{
			ClientId: t.Name(),
			Request: &btpb.ExecuteQueryRequest{
				InstanceName: instanceName,
				Query:        strconv.Itoa(i),
			},
		}
	}

	results := doExecuteQueryOps(t, server, reqs, nil)

	assert.Equal(t, concurrency, len(recorder))

	// request1
	checkResultOkStatus(t, results[0])
	assert.Equal(t, len(results[0].Metadata.Columns), 1)
	assert.True(t, cmp.Equal(results[0].Metadata, testProxyMd(column("strCol", strType())), protocmp.Transform()))
	assert.Equal(t, len(results[0].Rows), 3)
	assertRowEqual(t, testProxyRow(strVal("foo")), results[0].Rows[0], results[0].Metadata)
	assertRowEqual(t, testProxyRow(strVal("bar")), results[0].Rows[1], results[0].Metadata)
	assertRowEqual(t, testProxyRow(strVal("baz")), results[0].Rows[2], results[0].Metadata)

	// request2
	checkResultOkStatus(t, results[1])
	assert.Equal(t, len(results[1].Metadata.Columns), 2)
	assert.True(t, cmp.Equal(results[1].Metadata, testProxyMd(column("intCol", int64Type()), column("boolCol", boolType())), protocmp.Transform()))
	assert.Equal(t, len(results[1].Rows), 2)
	assertRowEqual(t, testProxyRow(intVal(1), boolVal(true)), results[1].Rows[0], results[1].Metadata)
	assertRowEqual(t, testProxyRow(intVal(2), boolVal(false)), results[1].Rows[1], results[1].Metadata)

	// request3
	checkResultOkStatus(t, results[2])
	assert.Equal(t, len(results[2].Metadata.Columns), 2)
	assert.True(t, cmp.Equal(results[2].Metadata, testProxyMd(column("mapCol", mapType(strType(), strType())), column("strCol", strType())), protocmp.Transform()))
	assert.Equal(t, len(results[2].Rows), 1)
	assertRowEqual(t, testProxyRow(mapVal(mapEntry(strVal("k"), strVal("v"))), strVal("foo")), results[2].Rows[0], results[2].Metadata)

	// request4
	checkResultOkStatus(t, results[3])
	assert.Equal(t, len(results[3].Metadata.Columns), 2)
	assert.True(t, cmp.Equal(results[3].Metadata, testProxyMd(column("strCol", strType()), column("bytesCol", bytesType())), protocmp.Transform()))
	assert.Equal(t, len(results[3].Rows), 0)

	// request5
	checkResultOkStatus(t, results[4])
	assert.Equal(t, len(results[4].Metadata.Columns), 1)
	assert.True(t, cmp.Equal(results[4].Metadata, testProxyMd(column("arrayOfString", arrayType(strType()))), protocmp.Transform()))
	assert.Equal(t, len(results[4].Rows), 2)
	assertRowEqual(t, testProxyRow(arrayVal(strVal("e1"), strVal("e2"))), results[4].Rows[0], results[4].Metadata)
	assertRowEqual(t, testProxyRow(arrayVal(strVal("f1"), strVal("f2"))), results[4].Rows[1], results[4].Metadata)
}

// tests that client doesn't kill inflight requests after client closing, but will reject new requests.
func TestExecuteQuery_CloseClient(t *testing.T) {
	clientID := t.Name()
	recorder := make(chan *executeQueryReqRecord, 4)

	repeatedAction := []*executeQueryAction{
		&executeQueryAction{
			response:    md(column("strCol", strType())),
			delayStr:    "2s",
			endOfStream: false,
		},
		&executeQueryAction{
			response:    partialResultSet("token", strVal("foo")),
			endOfStream: true,
		},
	}

	actionSequences := [][]*executeQueryAction{
		repeatedAction, repeatedAction, repeatedAction, repeatedAction,
	}
	server := initMockServer(t)
	server.ExecuteQueryFn = mockExecuteQueryFnWithMetadata(recorder, nil, actionSequences)

	// Will be finished
	reqsBatchOne := []*testproxypb.ExecuteQueryRequest{
		&testproxypb.ExecuteQueryRequest{
			ClientId: t.Name(),
			Request: &btpb.ExecuteQueryRequest{
				InstanceName: instanceName,
				Query:        "0",
			},
		},
		&testproxypb.ExecuteQueryRequest{
			ClientId: t.Name(),
			Request: &btpb.ExecuteQueryRequest{
				InstanceName: instanceName,
				Query:        "1",
			},
		},
	}
	// Will be rejected by client
	reqsBatchTwo := []*testproxypb.ExecuteQueryRequest{
		&testproxypb.ExecuteQueryRequest{
			ClientId: t.Name(),
			Request: &btpb.ExecuteQueryRequest{
				InstanceName: instanceName,
				Query:        "2",
			},
		},
		&testproxypb.ExecuteQueryRequest{
			ClientId: t.Name(),
			Request: &btpb.ExecuteQueryRequest{
				InstanceName: instanceName,
				Query:        "4",
			},
		},
	}

	setUp(t, server, clientID, nil)
	defer tearDown(t, server, clientID)

	closeClientAfter := time.Second
	resultsBatchOne := doExecuteQueryOpsCore(t, clientID, reqsBatchOne, &closeClientAfter)
	resultsBatchTwo := doExecuteQueryOpsCore(t, clientID, reqsBatchTwo, nil)

	// Check that server only receives batch-one requests
	assert.Equal(t, 2, len(recorder))

	// Check that all the batch-one requests succeeded or were cancelled
	checkResultOkOrCancelledStatus(t, resultsBatchOne...)
	for i := 0; i < 2; i++ {
		assert.NotNil(t, resultsBatchOne[i])
		if resultsBatchOne[i] == nil {
			continue
		}
		resCode := resultsBatchOne[i].GetStatus().GetCode()
		if resCode == int32(codes.Canceled) {
			continue
		}
		assert.Equal(t, len(resultsBatchOne[i].Metadata.Columns), 1)
		assert.True(t, cmp.Equal(resultsBatchOne[i].Metadata, testProxyMd(column("strCol", strType())), protocmp.Transform()))
		assert.Equal(t, len(resultsBatchOne[i].Rows), 1)
		assertRowEqual(t, testProxyRow(strVal("foo")), resultsBatchOne[i].Rows[0], resultsBatchOne[i].Metadata)
	}

	// Check that all the batch-two requests failed at the proxy level
	assert.NotEmpty(t, resultsBatchTwo[0].GetStatus().GetCode())
	assert.NotEmpty(t, resultsBatchTwo[1].GetStatus().GetCode())
}
