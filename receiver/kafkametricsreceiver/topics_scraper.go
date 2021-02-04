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
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/receiver/scraperhelper"
)

type topicsScraper struct {
	client      sarama.Client
	logger      *zap.Logger
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
	metrics.Resize(2)

	topicPartitionsMetric := metrics.At(0)
	topicCurrentOffsetMetric := metrics.At(1)
	initTopicPartitionsMetric(&topicPartitionsMetric)
	initTopicCurrentOffsetMetric(&topicCurrentOffsetMetric)

	var matchedTopics []string
	for _, t := range topics {
		if s.topicFilter.MatchString(t) {
			matchedTopics = append(matchedTopics, t)
		}
	}
	topicPartitionsMetric.IntGauge().DataPoints().Resize(len(matchedTopics))

	for topicIdx, topic := range matchedTopics {
		partitions, err := s.client.Partitions(topic)

		if err != nil {
			s.logger.Error("topics scraper: failed to get topic partitions", zap.String("Topic", topic), zap.Error(err))
			continue
		}
		addPartitionCountToMetric(topic, int64(len(partitions)), &topicPartitionsMetric, topicIdx)

		for _, partition := range partitions {
			currentOffset, err := s.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				s.logger.Error(
					"topics scraper: failed to get offset",
					zap.String("topic ", topic),
					zap.String("partition ", string(partition)),
					zap.Error(err))
				continue
			}
			addTopicCurrentOffsetToMetric(topic, partition, currentOffset, &topicCurrentOffsetMetric)
		}
	}
	return metrics, nil
}

func initTopicPartitionsMetric(m *pdata.Metric) {
	m.SetName("kafka_topics_partition")
	m.SetDescription("number of partitions for this topic")
	m.SetDataType(pdata.MetricDataTypeIntGauge)
}

func addPartitionCountToMetric(topic string, partitions int64, m *pdata.Metric, topicIdx int) {
	dp := m.IntGauge().DataPoints().At(topicIdx)
	dp.SetValue(partitions)
	dp.SetTimestamp(timeToUnixNano(time.Now()))
	dp.LabelsMap().InitFromMap(map[string]string{
		"topic": topic,
	})
}

func initTopicCurrentOffsetMetric(m *pdata.Metric) {
	m.SetName("kafka_topics_current_offset")
	m.SetDescription("current offset of topic/partition")
	m.SetDataType(pdata.MetricDataTypeIntGauge)
}

func addTopicCurrentOffsetToMetric(topic string, partition int32, currentOffset int64, m *pdata.Metric) {
	dpLen := m.IntGauge().DataPoints().Len()
	m.IntGauge().DataPoints().Resize(dpLen + 1)
	dp := m.IntGauge().DataPoints().At(dpLen)
	dp.SetValue(currentOffset)
	dp.SetTimestamp(timeToUnixNano(time.Now()))
	dp.LabelsMap().InitFromMap(map[string]string{
		"topic":     topic,
		"partition": strconv.FormatInt(int64(partition), 10),
	})
}

func createTopicsScraper(_ context.Context, config Config, client sarama.Client, logger *zap.Logger) scraperhelper.MetricsScraper {
	topicFilter := regexp.MustCompile(config.TopicMatch)
	s := topicsScraper{
		client:      client,
		logger:      logger,
		topicFilter: topicFilter,
	}
	ms := scraperhelper.NewMetricsScraper(s.Name(), s.scrape)
	return ms
}
