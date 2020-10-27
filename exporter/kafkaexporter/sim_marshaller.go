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
	"encoding/json"
	"errors"
	"math"
	"strconv"

	"go.opentelemetry.io/collector/consumer/pdata"
)

const (
	MetricTypeCounter         = "counter"
	MetricTypeGauge           = "gauge"
	MetricTypeCumulativeCount = "cumulative_count"
)

var infinityBound = math.Inf(1)

// SIMMetric is a JSON interface closely matching SIM metrics structure.
// Ref: https://dev.splunk.com/observability/docs/apibasics/data_model_basics
//	For datapoints of type pdata.MetricDataTypeIntHistogram and pdata.MetricDataTypeIntHistogram,
//	where <basename> is the root name of the metric:
//		The total count gets converted to a cumulative counter called <basename>_count
//		The total sum gets converted to a cumulative counter called <basename>
//		Each histogram bucket is converted to a cumulative counter called <basename>_bucket
//		and will include a dimension called upper_bound that specifies the maximum value in that bucket.
type SIMMetric struct {
	MetricName string                 `json:"metric_name"` // MetricName is the name of the metric
	Value      json.Number            `json:"value"`       // Value is the number value recorded for the metric
	MetricType string                 `json:"metric_type"` // MetricType is the type of the metric.
	Timestamp  json.Number            `json:"timestamp"`   // Timestamp  associated with metric.
	Dimensions map[string]interface{} `json:"dimensions"`  // Dimensions map of metadata (e.g. hostname) of metric.
}

type metricDP struct {
	time       json.Number
	labels     map[string]string
	metricType string
	value      json.Number
	nameSuffix string
}

type simJSONMarshaller struct{}

// Encoding returns config encoding for sim marshaller
func (m *simJSONMarshaller) Encoding() string {
	return "sim_json"
}

// Marshal encodes pdata.Metrics into []Message of JSON data
func (m *simJSONMarshaller) Marshal(metrics pdata.Metrics) ([]Message, error) {
	simMetrics, _ := MetricsToSIM(metrics)
	b, err := json.Marshal(*simMetrics)
	if err != nil {
		return nil, err
	}
	return []Message{{Value: b}}, nil
}

// MetricsToSIM encodes Metrics into arr of SIMMetric
func MetricsToSIM(md pdata.Metrics) (*[]SIMMetric, int) {
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
					droppedMetrics++
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
					metricName := metric.Name() + mdp.nameSuffix
					simMetric := SIMMetric{
						Value:      mdp.value,
						Timestamp:  mdp.time,
						MetricName: metricName,
						MetricType: mdp.metricType,
						Dimensions: dimensions,
					}
					mls = append(mls, simMetric)
				}
			}
		}
	}
	return &mls, droppedMetrics
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
				time:       intToNumber(int64(dp.Timestamp())),
				metricType: MetricTypeGauge,
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      intToNumber(dp.Value()),
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
				time:       intToNumber(int64(dp.Timestamp())),
				metricType: MetricTypeGauge,
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      floatToNumber(dp.Value()),
			}
			mdps = append(mdps, mdp)
		}
	case pdata.MetricDataTypeIntSum:
		intSum := m.IntSum()
		if intSum.IsNil() {
			return mdps, err
		}
		dataPoints := intSum.DataPoints()
		mType := MetricTypeCounter
		if intSum.AggregationTemporality() == pdata.AggregationTemporalityCumulative {
			mType = MetricTypeCumulativeCount
		}
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			if dp.IsNil() {
				continue
			}
			mdp := metricDP{
				time:       intToNumber(int64(dp.Timestamp())),
				metricType: mType,
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      intToNumber(dp.Value()),
			}
			mdps = append(mdps, mdp)
		}
	case pdata.MetricDataTypeDoubleSum:
		doubleSum := m.DoubleSum()
		if doubleSum.IsNil() {
			return mdps, err
		}
		dataPoints := doubleSum.DataPoints()
		mType := MetricTypeCounter
		if doubleSum.AggregationTemporality() == pdata.AggregationTemporalityCumulative {
			mType = MetricTypeCumulativeCount
		}
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			if dp.IsNil() {
				continue
			}
			mdp := metricDP{
				time:       intToNumber(int64(dp.Timestamp())),
				metricType: mType,
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      floatToNumber(dp.Value()),
			}
			mdps = append(mdps, mdp)
		}
	case pdata.MetricDataTypeDoubleHistogram:
		doubleHistogram := m.DoubleHistogram()
		if doubleHistogram.IsNil() {
			return nil, err
		}
		dataPoints := doubleHistogram.DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			if dp.IsNil() {
				continue
			}
			mdp := metricDP{
				time:       intToNumber(int64(dp.Timestamp())),
				metricType: MetricTypeCumulativeCount,
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      floatToNumber(dp.Sum()),
			}
			mdpCount := metricDP{
				time:       intToNumber(int64(dp.Timestamp())),
				metricType: MetricTypeCumulativeCount,
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      intToNumber(int64(dp.Count())),
				nameSuffix: "_count",
			}
			mdps = append(mdps, mdp, mdpCount)
			if len(dp.BucketCounts()) != len(dp.ExplicitBounds())+1 {
				continue
			}
			explicitBounds := append(dp.ExplicitBounds(), infinityBound)
			for i, upperBound := range explicitBounds {
				labels := map[string]string{
					"upper_bound": strconv.FormatFloat(upperBound, 'f', -1, 64),
				}
				for k, v := range dataPointDimensions(dp.LabelsMap()) {
					labels[k] = v
				}
				mdpQuantile := metricDP{
					time:       intToNumber(int64(dp.Timestamp())),
					metricType: MetricTypeCumulativeCount,
					labels:     labels,
					value:      intToNumber(int64(dp.BucketCounts()[i])),
					nameSuffix: "_bucket",
				}
				mdps = append(mdps, mdpQuantile)
			}
		}
	case pdata.MetricDataTypeIntHistogram:
		intHistogram := m.IntHistogram()
		if intHistogram.IsNil() {
			return nil, err
		}
		dataPoints := intHistogram.DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			if dp.IsNil() {
				continue
			}
			mdp := metricDP{
				time:       intToNumber(int64(dp.Timestamp())),
				metricType: MetricTypeCumulativeCount,
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      intToNumber(dp.Sum()),
			}
			mdpCount := metricDP{
				time:       intToNumber(int64(dp.Timestamp())),
				metricType: MetricTypeCumulativeCount,
				labels:     dataPointDimensions(dp.LabelsMap()),
				value:      intToNumber(int64(dp.Count())),
				nameSuffix: "_count",
			}
			mdps = append(mdps, mdp, mdpCount)
			if len(dp.BucketCounts()) != len(dp.ExplicitBounds())+1 {
				continue
			}
			explicitBounds := append(dp.ExplicitBounds(), infinityBound)
			for i, upperBound := range explicitBounds {
				labels := map[string]string{
					"upper_bound": strconv.FormatFloat(upperBound, 'f', -1, 64),
				}
				for k, v := range dataPointDimensions(dp.LabelsMap()) {
					labels[k] = v
				}
				mdpQuantile := metricDP{
					time:       intToNumber(int64(dp.Timestamp())),
					metricType: MetricTypeCumulativeCount,
					labels:     labels,
					value:      intToNumber(int64(dp.BucketCounts()[i])),
					nameSuffix: "_quantile",
				}
				mdps = append(mdps, mdpQuantile)
			}
		}
	default:
		return mdps, errors.New("invalid data type")
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
		return floatToNumber(val.DoubleVal())
	case pdata.AttributeValueBOOL:
		return val.BoolVal()
	case pdata.AttributeValueINT:
		return intToNumber(val.IntVal())
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

func intToNumber(n int64) json.Number {
	return json.Number(strconv.FormatInt(n, 10))
}

func floatToNumber(n float64) json.Number {
	return json.Number(strconv.FormatFloat(n, 'f', -1, 64))
}
