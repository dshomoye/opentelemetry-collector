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
	"github.com/Shopify/sarama"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"regexp"
	"time"
)

type topicsScraper struct {
}

func (s *topicsScraper) Name() string {
	return "topics"
}

func (s *topicsScraper) scrape(config Config, ctx context.Context, client sarama.Client, consumer consumer.MetricsConsumer, logger *zap.Logger) (*pdata.MetricSlice, error) {
	topics, err := client.Topics()
	if err != nil {
		logger.Error("Topics Scraper: Failed to refresh topics. Error: ", zap.Error(err))
		return nil, err
	}

	topicFilter := regexp.MustCompile(config.TopicMatch)
	metrics := pdata.NewMetricSlice()
	topicIdx := 0
	for _, topic := range topics {
		if topicFilter.MatchString(topic) {
			partitions, err := client.Partitions(topic)
			if err != nil {
				logger.Error("Topics Scraper: Failed to get topic partitions", zap.String("Topic", topic), zap.Error(err))
			}
			metrics.Resize(topicIdx + 1)
			topicPartitionsMetric := metrics.At(topicIdx)
			topicPartitionsMetric.SetDescription("Number of partitions for this topic")
			topicPartitionsMetric.IntGauge().DataPoints().Resize(1)
			dp := topicPartitionsMetric.IntGauge().DataPoints().At(0)
			now := TimeToUnixNano(time.Now())
			dp.SetValue(int64(len(partitions)))
			dp.SetTimestamp(now)

			topicIdx += 1
			//	TODO: insert labels for dp here?

		}
	}
	return &metrics, nil
}

//type topicsMetricsScraper struct {
//	client sarama.Client
//}
//
//func createTopicsScraper(ctx context.Context, client sarama.Client, logger *zap.Logger) (scraperhelper.MetricsScraper, error) {
//
//}