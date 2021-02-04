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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/receiver/scraperhelper"
)

const (
	brokersMetricName        = "kafka_brokers"
	brokersMetricDescription = "number of brokers in the cluster"
)

type brokersScraper struct {
	client       sarama.Client
	logger       *zap.Logger
	saramaConfig *sarama.Config
	config       Config
}

func (s *brokersScraper) Name() string {
	return "brokers"
}

func (s *brokersScraper) start(_ context.Context, _ component.Host) error {
	if s.client.Closed() {
		client, err := sarama.NewClient(s.config.Brokers, s.saramaConfig)
		if err != nil {
			return err
		}
		s.client = client
	}
	return nil
}

func (s *brokersScraper) shutdown(context.Context) error {
	if !s.client.Closed() {
		return s.client.Close()
	}
	return nil
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

func createBrokersScraper(_ context.Context, config Config, saramaConfig *sarama.Config, logger *zap.Logger) (scraperhelper.MetricsScraper, error) {
	client, err := sarama.NewClient(config.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}
	s := brokersScraper{
		client:       client,
		logger:       logger,
		config:       config,
		saramaConfig: saramaConfig,
	}
	ms := scraperhelper.NewMetricsScraper(
		s.Name(),
		s.scrape,
		scraperhelper.WithStart(s.start),
		scraperhelper.WithShutdown(s.shutdown),
	)
	return ms, nil
}
