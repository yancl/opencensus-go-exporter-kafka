// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	dumppb "github.com/yancl/opencensus-go-exporter-kafka/gen-go/dump/v1"
	"github.com/yancl/opencensus-go-exporter-kafka/internal/testpb"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/trace"
	"golang.org/x/net/context/ctxhttp"
)

const (
	defaultChainBufferSize = 100
)

func TestExport(t *testing.T) {
	c := make(chan *sarama.ProducerMessage, defaultChainBufferSize)
	p := newMockAsyncProducer(c)
	var exportErrors []error
	exporter, err := NewExporter(Options{
		Brokers: []string{"127.0.0.1:9092", "127.0.0.1:9093"},
		Topic:   "oc-kafka",
		OnError: func(err error) {
			exportErrors = append(exportErrors, err)
		},
		p: p,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Flush()

	trace.RegisterExporter(exporter)
	defer trace.UnregisterExporter(exporter)

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

	_, span := trace.StartSpan(context.Background(), "custom-span")
	time.Sleep(10 * time.Millisecond)
	span.End()

	// Test HTTP spans
	handler := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		_, backgroundSpan := trace.StartSpan(context.Background(), "BackgroundWork")
		spanContext := backgroundSpan.SpanContext()
		time.Sleep(10 * time.Millisecond)
		backgroundSpan.End()

		_, span := trace.StartSpan(req.Context(), "Sleep")
		span.AddLink(trace.Link{Type: trace.LinkTypeChild, TraceID: spanContext.TraceID, SpanID: spanContext.SpanID})
		time.Sleep(150 * time.Millisecond) // do work
		span.End()
		rw.Write([]byte("Hello, world!"))
	})
	server := httptest.NewServer(&ochttp.Handler{Handler: handler})
	defer server.Close()

	ctx := context.Background()
	client := &http.Client{
		Transport: &ochttp.Transport{},
	}
	resp, err := ctxhttp.Get(ctx, client, server.URL+"/test/123?abc=xyz")
	if err != nil {
		t.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := "Hello, world!", string(body); want != got {
		t.Fatalf("resp.Body = %q; want %q", want, got)
	}

	exporter.Flush()

	for _, err := range exportErrors {
		t.Error(err)
	}

	// close c in order not to block range c
	close(c)

	gotSpansNames, err := getSpanNames(c)
	if err != nil {
		t.Fatal(err)
	}

	// There are two "/test/123"
	// one for client and one for server
	wantSpanNames := []string{"custom-span", "BackgroundWork", "Sleep", "/test/123", "/test/123"}
	if !reflect.DeepEqual(gotSpansNames, wantSpanNames) {
		t.Errorf("span names got:%v, want:%v", gotSpansNames, wantSpanNames)
	}
}

func TestGRPC(t *testing.T) {
	c := make(chan *sarama.ProducerMessage, defaultChainBufferSize)
	p := newMockAsyncProducer(c)
	var exportErrors []error
	exporter, err := NewExporter(Options{
		Brokers: []string{"127.0.0.1:9092", "127.0.0.1:9093"},
		Topic:   "oc-kafka",
		OnError: func(err error) {
			exportErrors = append(exportErrors, err)
		},
		p: p,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Flush()

	trace.RegisterExporter(exporter)
	defer trace.UnregisterExporter(exporter)

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

	client, done := testpb.NewTestClient(t)
	defer done()

	client.Single(context.Background(), &testpb.FooRequest{SleepNanos: int64(42 * time.Millisecond)})

	exporter.Flush()

	for _, err := range exportErrors {
		t.Error(err)
	}

	// close c in order not to block range c
	close(c)

	gotSpansNames, err := getSpanNames(c)
	if err != nil {
		t.Fatal(err)
	}

	// There are two "testpb.Foo.Single"
	// one for client and one for server
	wantSpanNames := []string{"testpb.Single.Sleep", "testpb.Foo.Single", "testpb.Foo.Single"}
	if !reflect.DeepEqual(gotSpansNames, wantSpanNames) {
		t.Errorf("span names got:%v, want:%v", gotSpansNames, wantSpanNames)
	}
}

func getSpanNames(c <-chan *sarama.ProducerMessage) ([]string, error) {
	var spanNames []string
	for m := range c {
		ds := &dumppb.DumpSpans{}
		bs, err := m.Value.Encode()
		if err != nil {
			return nil, err
		}

		err = proto.Unmarshal(bs, ds)
		if err != nil {
			return nil, err
		}
		for _, span := range ds.GetSpans() {
			spanNames = append(spanNames, span.GetName().GetValue())
		}
	}
	return spanNames, nil
}
