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
	"go.opentelemetry.io/collector/consumer/pdata"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.opentelemetry.io/collector/internal/data/testdata"
)

var (
	TestMetricStartTime      = time.Date(2020, 10, 21, 20, 26, 32, 524, time.UTC)
	TestMetricStartTimestamp = pdata.TimestampUnixNano(TestMetricStartTime.UnixNano())
	TestMetricTime           = time.Date(2020, 10, 21, 20, 26, 33, 889, time.UTC)
	TestMetricTimestamp      = pdata.TimestampUnixNano(TestMetricTime.UnixNano())
)

func TestMetricsToSIM(t *testing.T) {
	md := generateMetricsAllTypesWithDataPoints()

	simList, _ := MetricsToSIM(md)
	result := *simList
	assert.Equal(t, 12, len(result))

	dgMetric := SIMMetric{
		MetricName: "gauge-double",
		Value:      "100",
		MetricType: "gauge",
		Timestamp:  "1603311993000000889",
		Dimensions: map[string]interface{}{
			"resource-attr": "resource-attr-val-1",
		},
	}
	assert.Equal(t, dgMetric, result[0])
}

func TestMetricsToSIM_ignores_invalid_dataType(t *testing.T) {
	md := testdata.GenerateMetricsMetricTypeInvalid()
	simList, dropped := MetricsToSIM(md)
	assert.Equal(t, 1, dropped)
	assert.Equal(t, 0, len(*simList))
}

func TestMetricsToSIM_drops_nil_metrics(t *testing.T) {
	md := testdata.GenerateMetricsOneMetricOneNil()
	simList, dropped := MetricsToSIM(md)
	assert.Equal(t, dropped, 1)
	assert.Greater(t, len(*simList), 0)
}

func TestMetricsToSIM_ignores_nil_dataPoints(t *testing.T) {
	md := testdata.GenerateMetricsAllTypesNilDataPoint()
	simList, _ := MetricsToSIM(md)
	assert.Equal(t, 0, len(*simList))
}

func TestMetricsToSIM_parses_all_attributes(t *testing.T) {
	md := testdata.GenerateMetricsOneMetricOneDataPoint()
	resource := md.ResourceMetrics().At(0).Resource()
	resource.Attributes().InitFromAttributeMap(*getAttributesAllTypes())
	intGauge := md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0).IntGauge()
	intGauge.DataPoints().At(0).LabelsMap().InitEmptyWithCapacity(0)

	m, _ := MetricsToSIM(md)
	result := *m
	size := len(result[0].Dimensions)
	assert.Equal(t, 6, size)
}

func initIntGaugeMetricOneDataPoint(m pdata.Metric) {
	dps := m.IntGauge().DataPoints()
	dps.Resize(1)
	dp0 := dps.At(0)
	dp0.SetStartTime(TestMetricStartTimestamp)
	dp0.SetTimestamp(TestMetricTimestamp)
	dp0.SetValue(50)
}

func initDoubleGaugeMetricOneDataPoint(m pdata.Metric) {
	dps := m.DoubleGauge().DataPoints()
	dps.Resize(1)
	dp0 := dps.At(0)
	dp0.SetStartTime(TestMetricStartTimestamp)
	dp0.SetTimestamp(TestMetricTimestamp)
	dp0.SetValue(100)

}

func initIntSumOneDataPoint(m pdata.Metric) {
	dps := m.IntSum().DataPoints()
	dps.Resize(1)
	dp0 := dps.At(0)
	dp0.SetTimestamp(TestMetricTimestamp)
	dp0.SetValue(150)
}

func initDoubleSumOneDataPoint(m pdata.Metric) {
	dps := m.DoubleSum().DataPoints()
	dps.Resize(1)
	dp0 := dps.At(0)
	dp0.SetTimestamp(TestMetricTimestamp)
	dp0.SetValue(200)
}

func initIntHistogramDataPoint(hm pdata.Metric) {
	hdps := hm.IntHistogram().DataPoints()
	hdps.Resize(1)
	hdp1 := hdps.At(0)
	hdp1.SetStartTime(TestMetricStartTimestamp)
	hdp1.SetTimestamp(TestMetricTimestamp)
	hdp1.SetCount(1)
	hdp1.SetSum(15)
	hdp1.SetBucketCounts([]uint64{0, 1})
	hdp1.SetExplicitBounds([]float64{1})
}

func initDoubleHistogramDataPoint(hm pdata.Metric) {
	hdps := hm.DoubleHistogram().DataPoints()
	hdps.Resize(1)
	hdp1 := hdps.At(0)
	hdp1.SetStartTime(TestMetricStartTimestamp)
	hdp1.SetTimestamp(TestMetricTimestamp)
	hdp1.SetCount(1)
	hdp1.SetSum(15)
	hdp1.SetBucketCounts([]uint64{0, 1})
	hdp1.SetExplicitBounds([]float64{1})
}

func generateMetricsAllTypesWithDataPoints() pdata.Metrics {
	metricData := testdata.GenerateMetricsAllTypesEmptyDataPoint()
	metrics := metricData.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics()
	for i := 0; i < metrics.Len(); i++ {
		metric := metrics.At(i)
		switch metric.DataType() {
		case pdata.MetricDataTypeIntGauge:
			initIntGaugeMetricOneDataPoint(metric)
		case pdata.MetricDataTypeDoubleGauge:
			initDoubleGaugeMetricOneDataPoint(metric)
		case pdata.MetricDataTypeDoubleSum:
			initDoubleSumOneDataPoint(metric)
		case pdata.MetricDataTypeIntSum:
			initIntSumOneDataPoint(metric)
		case pdata.MetricDataTypeIntHistogram:
			initIntHistogramDataPoint(metric)
		case pdata.MetricDataTypeDoubleHistogram:
			initDoubleHistogramDataPoint(metric)
		}
	}
	return metricData
}

func getAttributesAllTypes() *pdata.AttributeMap {
	am := pdata.NewAttributeMap()
	am.InsertBool("bool-key", false)
	am.InsertDouble("double-key", 90)
	am.InsertInt("int-key", 91)
	am.InsertNull("null-key")
	am.InsertString("string-key", "string")
	arrVal := pdata.NewAttributeValueArray()
	extraAttr1 := pdata.NewAnyValueArray()
	extraAttr1.Append(pdata.NewAttributeValueString("arr-string"))
	extraAttr1.Append(pdata.NewAttributeValueInt(0))
	extraAttr1.Append(pdata.NewAttributeValueBool(true))
	arrVal.SetArrayVal(extraAttr1)
	am.Insert("arr", arrVal)
	return &am
}
