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

package kafka_test

import (
	"log"
	"net/http"

	"github.com/yancl/opencensus-go-exporter-kafka"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/trace"
)

func Example_defaults() {
	exporter, err := kafka.NewExporter(
		kafka.Options{
			Brokers: []string{"127.0.0.1:9092"},
			Topic:   "oc-kafka",
		})

	if err != nil {
		log.Fatal(err)
	}

	// Export to Kafka Trace.
	trace.RegisterExporter(exporter)

	// Use default B3 format header to outgoing requests:
	client := &http.Client{
		Transport: &ochttp.Transport{},
	}
	_ = client // use client

	// All outgoing requests from client will include a B3(default) Trace header.
	// See the ochttp package for how to handle incoming requests.
}
