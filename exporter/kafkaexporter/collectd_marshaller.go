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

// JsonValueList represents the format used by collectd's JSON export.
type JsonValueList struct {
	Values         []json.Number          `json:"values"`
	DSTypes        []string               `json:"dstypes"`
	DSNames        []string               `json:"dsnames,omitempty"`
	Time           json.Number            `json:"time"`
	Interval       json.Number            `json:"interval,omitempty"`
	Host           string                 `json:"host,omitempty"`
	Plugin         string                 `json:"plugin"`
	PluginInstance string                 `json:"plugin_instance,omitempty"`
	Type           string                 `json:"type"`
	TypeInstance   string                 `json:"type_instance,omitempty"`
	Meta           map[string]interface{} `json:"meta"`
}

type metricDP struct {
	values  []json.Number
	dstypes []string
	dsnames []string
	time    json.Number
}

// MetricsToValueLists encodes Metrics into []JsonValueList
// each value list will contain a host key if the underlying resource for a metric has a "host.hostname" attribute
func MetricsToValueLists(md pdata.Metrics) ([]JsonValueList, int) {
	var droppedMetrics int
	var vls []JsonValueList
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		if rm.IsNil() {
			continue
		}
		ilms := rm.InstrumentationLibraryMetrics()
		resource := rm.Resource()
		meta := resourceMeta(resource)
		host, hasHost := getHostName(resource)
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
					valueList := JsonValueList{
						Values:  mdp.values,
						DSTypes: mdp.dstypes,
						DSNames: mdp.dsnames,
						Time:    mdp.time,
						Type:    metric.Name(),
						Plugin:  metric.Name(),
						Meta:    meta,
					}
					if hasHost {
						valueList.Host = host
					}
					vls = append(vls, valueList)
				}
			}
		}
	}
	return vls, droppedMetrics
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
			val := []json.Number{intToNumber(dp.Value())}
			mdp := metricDP{
				values:  val,
				dsnames: []string{"gauge"},
				dstypes: []string{"value"},
				time:    timeStampToNumber(dp.Timestamp()),
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
			val := []json.Number{floatToNumber(dp.Value())}
			mdp := metricDP{
				values:  val,
				dsnames: []string{"gauge"},
				dstypes: []string{"value"},
				time:    timeStampToNumber(dp.Timestamp()),
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
			val := []json.Number{intToNumber(dp.Value())}
			mdp := metricDP{
				values:  val,
				dsnames: []string{"counter"},
				dstypes: []string{"value"},
				time:    timeStampToNumber(dp.Timestamp()),
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
			val := []json.Number{floatToNumber(dp.Value())}
			mdp := metricDP{
				values:  val,
				dsnames: []string{"counter"},
				dstypes: []string{"value"},
				time:    timeStampToNumber(dp.Timestamp()),
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
	rattrs := resource.Attributes()
	rattrs.ForEach(func(key string, val pdata.AttributeValue) {
		meta[key] = getAttributeData(val)
	})
	return meta
}

func getHostName(resource pdata.Resource) (string, bool) {
	res := ""
	found := false
	if resource.IsNil() {
		return res, false
	}
	rattrs := resource.Attributes()
	rattrs.ForEach(func(key string, val pdata.AttributeValue) {
		if key == "host.hostname" {
			if val.Type() == pdata.AttributeValueSTRING {
				res = val.StringVal()
				found = true
			}
		}
	})
	return res, found
}

func floatToNumber(d float64) json.Number {
	return json.Number(strconv.FormatFloat(d, 'e', -1, 64))
}

func intToNumber(d int64) json.Number {
	return json.Number(strconv.FormatInt(d, 10))
}

func timeStampToNumber(d pdata.TimestampUnixNano) json.Number {
	return json.Number(strconv.FormatInt(int64(d), 10))
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
