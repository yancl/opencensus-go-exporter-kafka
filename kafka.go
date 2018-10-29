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

// Package kafka contains the OpenCensus exporters for Tracing.
//
// This exporter can be used to send traces to kafka brokers.
//
// Most of the codes base on https://github.com/census-ecosystem/opencensus-go-exporter-stackdriver.git
package kafka // import "github.com/yancl/opencensus-go-exporter-kafka"

import (
	"context"
	"fmt"
	"github.com/golang/glog"
	"time"

	"github.com/Shopify/sarama"
	"go.opencensus.io/trace"
)

// Options contains options for configuring the exporter.
type Options struct {
	// Brokers is the addresses of the kafka brokers
	// Required
	Brokers []string

	// Topic is the topic of kafka to send spans to
	// Required
	Topic string

	// OnError is the hook to be called when there is
	// an error uploading the tracing data.
	// If no custom hook is set, errors are logged.
	// Optional.
	OnError func(err error)

	// BundleDelayThreshold determines the max amount of time
	// the exporter can wait before uploading view data to
	// the backend.
	// Optional.
	BundleDelayThreshold time.Duration

	// BundleCountThreshold determines how many view data events
	// can be buffered before batch uploading them to the backend.
	// Optional.
	BundleCountThreshold int

	// DefaultTraceAttributes will be appended to every span that is exported to
	// Kafka Trace.
	DefaultTraceAttributes map[string]interface{}

	// Context allows users to provide a custom context for API calls.
	//
	// This context will be used several times: first, to create Kafka
	// trace and metric clients, and then every time a new batch of traces or
	// stats needs to be uploaded.
	//
	// If unset, context.Background() will be used.
	Context context.Context

	// p as a hook point allows mock for test
	p sarama.AsyncProducer
}

// Exporter is a trace.Exporter
// implementation that uploads data to Kafka.
type Exporter struct {
	traceExporter *traceExporter
}

// NewExporter creates a new Exporter that implements trace.Exporter.
func NewExporter(o Options) (*Exporter, error) {
	if o.Context == nil {
		o.Context = context.Background()
	}

	if o.Brokers == nil {
		return nil, fmt.Errorf("opencensus kafka exporter: broker addrs are empty")
	}

	if o.Topic == "" {
		return nil, fmt.Errorf("opencensus kafka exporter: topic are empty")
	}

	te, err := newTraceExporter(o)
	if err != nil {
		return nil, err
	}

	return &Exporter{
		traceExporter: te,
	}, nil
}

// ExportSpan exports a SpanData to Kafka.
func (e *Exporter) ExportSpan(sd *trace.SpanData) {
	if len(e.traceExporter.o.DefaultTraceAttributes) > 0 {
		sd = e.sdWithDefaultTraceAttributes(sd)
	}
	e.traceExporter.ExportSpan(sd)
}

func (e *Exporter) sdWithDefaultTraceAttributes(sd *trace.SpanData) *trace.SpanData {
	newSD := *sd
	newSD.Attributes = make(map[string]interface{})
	for k, v := range e.traceExporter.o.DefaultTraceAttributes {
		newSD.Attributes[k] = v
	}
	for k, v := range sd.Attributes {
		newSD.Attributes[k] = v
	}
	return &newSD
}

// Flush waits for exported data to be uploaded.
//
// This is useful if your program is ending and you do not
// want to lose recent stats or spans.
func (e *Exporter) Flush() {
	e.traceExporter.Flush()
}

func (o Options) handleError(err error) {
	if o.OnError != nil {
		o.OnError(err)
		return
	}
	glog.Warningf("Failed to export to Kafka: %v", err)
}
