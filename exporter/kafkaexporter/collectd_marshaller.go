// Copyright  The OpenTelemetry Authors
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
	"errors"
	"strconv"

	"encoding/json"
	"go.opentelemetry.io/collector/consumer/pdata"
)

// SIMMetric is a JSON interface closely matching SIM metrics structure
// ref: https://dev.splunk.com/observability/docs/apibasics/data_model_basics
type SIMMetric struct {
	MetricName string                 `json:"metric_name"`
	Value      json.Number            `json:"value"`
	MetricType string                 `json:"metric_type"`
	Timestamp  json.Number            `json:"timestamp"`
	Dimensions map[string]interface{} `json:"dimensions"`
}

type metricDP struct {
	time       json.Number
	labels     map[string]string
	metricType string
	value      json.Number
}

type collectdJSONMarshaller struct {}

// Encoding returns config encoding for collectd marshaller
func (m *collectdJSONMarshaller) Encoding() string {
	return "collectd"
}

// Marshal encodes pdata.Metrics into []Message of JSON data
func (m *collectdJSONMarshaller) Marshal(metrics pdata.Metrics) ([]Message, error) {
	simMetrics, _ := MetricsToValueLists(metrics)
	b, err := json.Marshal(simMetrics)
	if err != nil {
		return nil, err
	}
	return []Message{{Value: b}}, nil
}

// MetricsToValueLists encodes Metrics into arr of SIMMetric
// the SIMMetric.Dimensions property contains attributes of the associated resource and labels associated with the specific datapoint
func MetricsToValueLists(md pdata.Metrics) ([]SIMMetric, int) {
	var droppedMetrics int
	var mls []SIMMetric
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		if rm.IsNil() {
			continue
		}
		ilms := rm.InstrumentationLibraryMetrics()
		resource := rm.Resource()
		meta := resourceMeta(resource)
		for j := 0; j < ilms.Len(); j++ {
			ilm := ilms.At(j)
			if ilm.IsNil() {
				continue
			}
			metrics := ilm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				if metric.IsNil() {
					continue
				}
				metricValues, err := valuesForMetric(metric)
				if err != nil {
					droppedMetrics++
				}
				for _, mdp := range metricValues {
					dimensions := make(map[string]interface{})
					for k, v := range meta {
						dimensions[k] = v
					}
					for k, v := range mdp.labels {
						dimensions[k] = v
					}
					simMetric := SIMMetric{
						Value:      mdp.value,
						Timestamp:  mdp.time,
						MetricName: metric.Name(),
						MetricType: mdp.metricType,
						Dimensions: dimensions,
					}
					mls = append(mls, simMetric)
				}
			}
		}
	}
	return mls, droppedMetrics
}

func dataPointDimensions(sm pdata.StringMap) map[string]string {
	res := make(map[string]string)
	sm.ForEach(func(k string, v pdata.StringValue) {
		res[k] = v.Value()
	})
	return res
}

func valuesForMetric(m pdata.Metric) ([]metricDP, error) {
	var mdps []metricDP
	err := errors.New("nil metric")
	switch m.DataType() {
	case pdata.MetricDataTypeIntGauge:
		intGauge := m.IntGauge()
		if intGauge.IsNil() {
			return mdps, err
		}
		dataPoints := intGauge.DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			if dp.IsNil() {
				continue
			}
			mdp := metricDP{
				time:       json.Number(strconv.FormatInt(int64(dp.Timestamp()), 10)),
				metricType: "gauge",
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      json.Number(strconv.FormatInt(dp.Value(), 10)),
			}
			mdps = append(mdps, mdp)
		}
	case pdata.MetricDataTypeDoubleGauge:
		doubleGauge := m.DoubleGauge()
		if doubleGauge.IsNil() {
			return mdps, err
		}
		dataPoints := doubleGauge.DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			if dp.IsNil() {
				continue
			}
			mdp := metricDP{
				time:       json.Number(strconv.FormatInt(int64(dp.Timestamp()), 10)),
				metricType: "gauge",
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      json.Number(strconv.FormatFloat(dp.Value(), 'e', -1, 64)),
			}
			mdps = append(mdps, mdp)
		}
	case pdata.MetricDataTypeIntSum:
		intSum := m.IntSum()
		if intSum.IsNil() {
			return mdps, err
		}
		dataPoints := intSum.DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			if dp.IsNil() {
				continue
			}
			mdp := metricDP{
				time:       json.Number(strconv.FormatInt(int64(dp.Timestamp()), 10)),
				metricType: "counter",
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      json.Number(strconv.FormatInt(dp.Value(), 10)),
			}
			mdps = append(mdps, mdp)
		}
	case pdata.MetricDataTypeDoubleSum:
		doubleSum := m.DoubleSum()
		if doubleSum.IsNil() {
			return mdps, err
		}
		dataPoints := doubleSum.DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			if dp.IsNil() {
				continue
			}
			mdp := metricDP{
				time:       json.Number(strconv.FormatInt(int64(dp.Timestamp()), 10)),
				metricType: "counter",
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      json.Number(strconv.FormatFloat(dp.Value(), 'e', -1, 64)),
			}
			mdps = append(mdps, mdp)
		}
	}
	return mdps, nil
}

func resourceMeta(resource pdata.Resource) map[string]interface{} {
	meta := make(map[string]interface{})
	if resource.IsNil() {
		return meta
	}
	attrs := resource.Attributes()
	attrs.ForEach(func(key string, val pdata.AttributeValue) {
		meta[key] = getAttributeData(val)
	})
	return meta
}

func getAttributeData(val pdata.AttributeValue) interface{} {
	switch val.Type() {
	case pdata.AttributeValueSTRING:
		return val.StringVal()
	case pdata.AttributeValueDOUBLE:
		return json.Number(strconv.FormatFloat(val.DoubleVal(), 'e', -1, 64))
	case pdata.AttributeValueBOOL:
		return val.BoolVal()
	case pdata.AttributeValueINT:
		return json.Number(strconv.FormatInt(val.IntVal(), 10))
	case pdata.AttributeValueARRAY:
		valArray := val.ArrayVal()
		var dataArray []interface{}
		for i := 0; i < valArray.Len(); i++ {
			dataArray = append(dataArray, getAttributeData(valArray.At(i)))
		}
		return dataArray
	case pdata.AttributeValueMAP:
		return val.MapVal()
	}
	return nil
}
