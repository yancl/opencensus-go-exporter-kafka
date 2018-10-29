// Copyright 2017, OpenCensus Authors
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
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	dumppb "github.com/yancl/opencensus-go-exporter-kafka/gen-go/dump/v1"
	"go.opencensus.io/trace"
	"google.golang.org/api/support/bundler"
)

// traceExporter is an implementation of trace.Exporter that uploads spans to
// Kafka.
//
type traceExporter struct {
	topic   string
	o       Options
	bundler *bundler.Bundler
	// uploadFn defaults to uploadSpans; it can be replaced for tests.
	uploadFn func(spans []*trace.SpanData)
	overflowLogger
	producer sarama.AsyncProducer
}

var _ trace.Exporter = (*traceExporter)(nil)

func newTraceExporter(o Options) (*traceExporter, error) {
	var p sarama.AsyncProducer
	if o.p != nil {
		p = o.p
	} else {
		var err error
		if p, err = newAsyncProducer(o.Brokers); err != nil {
			return nil, fmt.Errorf("opencensus kafka exporter: couldn't initialize kafka producer: %v", err)
		}
	}
	return newTraceExporterWithClient(o, p), nil
}

func newAsyncProducer(brokerList []string) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("Failed to create Sarama kafka producer:%v", err)
	}

	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			glog.Warningf("kafka send message failed:%v", err)
		}
	}()

	return producer, nil
}

func newTraceExporterWithClient(o Options, p sarama.AsyncProducer) *traceExporter {
	e := &traceExporter{
		producer: p,
		o:        o,
		topic:    o.Topic,
	}
	bundler := bundler.NewBundler((*trace.SpanData)(nil), func(bundle interface{}) {
		e.uploadFn(bundle.([]*trace.SpanData))
	})
	if o.BundleDelayThreshold > 0 {
		bundler.DelayThreshold = o.BundleDelayThreshold
	} else {
		bundler.DelayThreshold = 2 * time.Second
	}
	if o.BundleCountThreshold > 0 {
		bundler.BundleCountThreshold = o.BundleCountThreshold
	} else {
		bundler.BundleCountThreshold = 50
	}
	// The measured "bytes" are not really bytes, see exportReceiver.
	bundler.BundleByteThreshold = bundler.BundleCountThreshold * 200
	bundler.BundleByteLimit = bundler.BundleCountThreshold * 1000
	bundler.BufferedByteLimit = bundler.BundleCountThreshold * 2000

	e.bundler = bundler
	e.uploadFn = e.uploadSpans
	return e
}

// ExportSpan exports a SpanData to Kafka.
func (e *traceExporter) ExportSpan(s *trace.SpanData) {
	// n is a length heuristic.
	n := 1
	n += len(s.Attributes)
	n += len(s.Annotations)
	n += len(s.MessageEvents)
	err := e.bundler.Add(s, n)
	switch err {
	case nil:
		return
	case bundler.ErrOversizedItem:
		go e.uploadFn([]*trace.SpanData{s})
	case bundler.ErrOverflow:
		e.overflowLogger.log()
	default:
		e.o.handleError(err)
	}
}

// Flush waits for exported trace spans to be uploaded.
//
// This is useful if your program is ending and you do not want to lose recent
// spans.
func (e *traceExporter) Flush() {
	e.bundler.Flush()
}

// uploadSpans uploads a set of spans to Kafka.
func (e *traceExporter) uploadSpans(spans []*trace.SpanData) {
	m := &dumppb.DumpSpans{Spans: make([]*tracepb.Span, 0, len(spans))}
	for _, span := range spans {
		m.Spans = append(m.Spans, protoFromSpanData(span))
	}

	if len(m.Spans) > 0 {
		// just use the trace id of the first span of the bundle
		// as the partition key to distribute the spans
		// around all partitions of the topic
		key := sarama.ByteEncoder(m.Spans[0].GetTraceId())
		data, err := dumpSpans(m)
		if err != nil {
			e.o.handleError(err)
			return
		}

		// send message to kafka
		e.producer.Input() <- &sarama.ProducerMessage{
			Topic: e.topic,
			Key:   key,
			Value: sarama.ByteEncoder(data),
		}
	}
}

func dumpSpans(ds *dumppb.DumpSpans) ([]byte, error) {
	serialized, err := proto.Marshal(ds)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

// overflowLogger ensures that at most one overflow error log message is
// written every 5 seconds.
type overflowLogger struct {
	mu    sync.Mutex
	pause bool
	accum int
}

func (o *overflowLogger) delay() {
	o.pause = true
	time.AfterFunc(5*time.Second, func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		switch {
		case o.accum == 0:
			o.pause = false
		case o.accum == 1:
			glog.Warningln("OpenCensus Kafka exporter: failed to upload span: buffer full")
			o.accum = 0
			o.delay()
		default:
			glog.Warningf("OpenCensus Kafka exporter: failed to upload %d spans: buffer full", o.accum)
			o.accum = 0
			o.delay()
		}
	})
}

func (o *overflowLogger) log() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.pause {
		glog.Warningln("OpenCensus Kafka exporter: failed to upload span: buffer full")
		o.delay()
	} else {
		o.accum++
	}
}
