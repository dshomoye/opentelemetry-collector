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

package kafkametricsreceiver

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/receiver/scraperhelper"
)

const (
	brokersMetricName        = "kafka_brokers"
	brokersMetricDescription = "number of brokers in the cluster"
)

type brokersScraper struct {
	client sarama.Client
	logger *zap.Logger
}

func (s *brokersScraper) Name() string {
	return "brokers"
}

func (s *brokersScraper) scrape(context.Context) (pdata.MetricSlice, error) {
	brokers := s.client.Brokers()
	metrics := pdata.NewMetricSlice()
	metrics.Resize(1)
	brokersMetrics := metrics.At(0)
	brokersMetrics.SetDescription(brokersMetricDescription)
	brokersMetrics.SetName(brokersMetricName)
	brokersMetrics.SetDataType(pdata.MetricDataTypeIntGauge)
	brokersMetrics.IntGauge().DataPoints().Resize(1)
	dp := brokersMetrics.IntGauge().DataPoints().At(0)
	dp.SetValue(int64(len(brokers)))
	dp.SetTimestamp(timeToUnixNano(time.Now()))
	return metrics, nil
}

func createBrokersScraper(_ context.Context, _ Config, client sarama.Client, logger *zap.Logger) scraperhelper.MetricsScraper {
	s := brokersScraper{
		client: client,
		logger: logger,
	}
	ms := scraperhelper.NewMetricsScraper(s.Name(), s.scrape)
	return ms
}
