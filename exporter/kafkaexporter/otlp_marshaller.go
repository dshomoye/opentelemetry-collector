// Copyright 2020 The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafkaexporter

import (
	"bytes"

	"github.com/gogo/protobuf/jsonpb"

	"go.opentelemetry.io/collector/consumer/pdata"
	otlpmetric "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/collector/metrics/v1"
	otlptrace "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/collector/trace/v1"
)

var jsonpbMarshaller = &jsonpb.Marshaler{}

type otlpTracesPbMarshaller struct {
}

type otlpMetricsPbMarshaller struct {
}

type otlpMetricsJSONMarshaller struct {
}

type otlpTracesJSONMarshaller struct {
}

var _ TracesMarshaller = (*otlpTracesPbMarshaller)(nil)

func (m *otlpTracesPbMarshaller) Encoding() string {
	return defaultEncoding
}

func (m *otlpMetricsJSONMarshaller) Encoding() string {
	return "otlp_json"
}

func (m *otlpTracesJSONMarshaller) Encoding() string {
	return "otlp_json"
}

func (m *otlpMetricsJSONMarshaller) Marshal(metrics pdata.Metrics) ([]Message, error) {
	out := new(bytes.Buffer)
	request := otlpmetric.ExportMetricsServiceRequest{
		ResourceMetrics: pdata.MetricsToOtlp(metrics),
	}
	err := jsonpbMarshaller.Marshal(out, &request)
	if err != nil {
		return nil, err
	}
	return []Message{{Value: out.Bytes()}}, nil
}

func (m *otlpTracesJSONMarshaller) Marshal(metrics pdata.Traces) ([]Message, error) {
	out := new(bytes.Buffer)
	request := otlptrace.ExportTraceServiceRequest{
		ResourceSpans: pdata.TracesToOtlp(metrics),
	}
	err := jsonpbMarshaller.Marshal(out, &request)
	if err != nil {
		return nil, err
	}
	return []Message{{Value: out.Bytes()}}, nil
}

func (m *otlpTracesPbMarshaller) Marshal(traces pdata.Traces) ([]Message, error) {
	request := otlptrace.ExportTraceServiceRequest{
		ResourceSpans: pdata.TracesToOtlp(traces),
	}
	bts, err := request.Marshal()
	if err != nil {
		return nil, err
	}
	return []Message{{Value: bts}}, nil
}

func (m *otlpMetricsPbMarshaller) Encoding() string {
	return defaultEncoding
}

func (m *otlpMetricsPbMarshaller) Marshal(metrics pdata.Metrics) ([]Message, error) {
	request := otlpmetric.ExportMetricsServiceRequest{
		ResourceMetrics: pdata.MetricsToOtlp(metrics),
	}
	bts, err := request.Marshal()
	if err != nil {
		return nil, err
	}
	return []Message{{Value: bts}}, nil
}
