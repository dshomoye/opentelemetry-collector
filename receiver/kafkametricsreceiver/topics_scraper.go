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
	"regexp"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/receiver/scraperhelper"
)

type topicsScraper struct {
	client sarama.Client
	config Config
	logger *zap.Logger
	topicFilter *regexp.Regexp
}

func (s *topicsScraper) Name() string {
	return "topics"
}

func (s *topicsScraper) scrape(context.Context) (pdata.MetricSlice, error) {
	topics, err := s.client.Topics()
	metrics := pdata.NewMetricSlice()
	if err != nil {
		s.logger.Error("Topics Scraper: Failed to refresh topics. Error: ", zap.Error(err))
		return metrics, err
	}
	topicIdx := 0
	for _, topic := range topics {
		if s.topicFilter.MatchString(topic) {
			partitions, err := s.client.Partitions(topic)
			if err != nil {
				s.logger.Error("Topics Scraper: Failed to get topic partitions", zap.String("Topic", topic), zap.Error(err))
			}
			metrics.Resize(topicIdx + 1)
			topicPartitionsMetric := metrics.At(topicIdx)
			topicPartitionsMetric.SetDescription("Number of partitions for this topic")
			topicPartitionsMetric.SetName("kafkametrics_topics_partition")
			topicPartitionsMetric.SetDataType(pdata.MetricDataTypeIntGauge)
			topicPartitionsMetric.IntGauge().DataPoints().Resize(1)
			dp := topicPartitionsMetric.IntGauge().DataPoints().At(0)
			dp.SetValue(int64(len(partitions)))
			dp.SetTimestamp(timeToUnixNano(time.Now()))
			dp.LabelsMap().InitFromMap(map[string]string{
				"topic": topic,
			})

			topicIdx++
		}
	}
	return metrics, nil
}

func createTopicsScraper(_ context.Context, config Config, client sarama.Client, logger *zap.Logger) scraperhelper.MetricsScraper {
	topicFilter := regexp.MustCompile(config.TopicMatch)
	s := topicsScraper{
		client: client,
		config: config,
		logger: logger,
		topicFilter: topicFilter,
	}
	ms := scraperhelper.NewMetricsScraper(s.Name(), s.scrape)
	return ms
}
