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
	"fmt"
	"github.com/Shopify/sarama"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/kafkaexporter"
	"go.uber.org/zap"
	"time"
)

type KafkaMetricScraper interface {
	Name() string
	scrape(config Config, ctx context.Context, client sarama.Client, consumer consumer.MetricsConsumer, logger *zap.Logger) (*pdata.MetricSlice, error)
}

type kafkaMetricsConsumer struct {
	name     string
	consumer consumer.MetricsConsumer
	cancel   context.CancelFunc
	scrapers []KafkaMetricScraper
	logger   *zap.Logger
	client   sarama.Client
	config   Config
}

func newMetricsReceiver(config Config, params component.ReceiverCreateParams, consumer consumer.MetricsConsumer) (*kafkaMetricsConsumer, error) {
	c := sarama.NewConfig()
	c.ClientID = config.ClientID
	if config.ProtocolVersion != "" {
		version, err := sarama.ParseKafkaVersion(config.ProtocolVersion)
		if err != nil {
			return nil, err
		}
		c.Version = version
	}
	if err := kafkaexporter.ConfigureAuthentication(config.Authentication, c); err != nil {
		return nil, err
	}
	_, err := sarama.NewClient(config.Brokers, c)
	if err != nil {
		return nil, err
	}

	scrapersMap := map[string]KafkaMetricScraper{
		"topics": &topicsScraper{},
	}
	var scrapers []KafkaMetricScraper
	for _, scraper := range config.Scrapers {
		if s, ok := scrapersMap[scraper]; ok {
			scrapers = append(scrapers, s)
		}
	}

	if len(scrapers) < 1 {
		return nil, fmt.Errorf("invalid configuration: no known scrapers found")
	}

	return &kafkaMetricsConsumer{
		name:     config.Name(),
		consumer: consumer,
		scrapers: scrapers,
		logger:   params.Logger,
		config:   config,
	}, nil
}

func (c *kafkaMetricsConsumer) Start(ctx context.Context, _ component.Host) error {
	rms := pdata.NewResourceMetricsSlice()
	rms.Resize(1)
	rm := rms.At(0)
	ilms := rm.InstrumentationLibraryMetrics()
	ilms.Resize(1)
	ilm := ilms.At(0)
	metrics := pdata.NewMetrics()

	for _, scraper := range c.scrapers {
		m, err := scraper.scrape(c.config, ctx, c.client, c.consumer, c.logger)
		if err != nil {
			c.logger.Error("error occurred during scraping. ", zap.String("scraper: ", scraper.Name()), zap.Error(err))
			continue
		}
		m.MoveAndAppendTo(ilm.Metrics())
	}
	rms.MoveAndAppendTo(metrics.ResourceMetrics())
	err := c.consumer.ConsumeMetrics(ctx, metrics)
	if err != nil {
		c.logger.Error("error consuming metrics ", zap.Error(err))
		return err
	}
	return nil
}

func (c *kafkaMetricsConsumer) Shutdown(ctx context.Context) error {
	c.cancel()
	return c.client.Close()
}

func TimeToUnixNano(t time.Time) pdata.TimestampUnixNano {
	return pdata.TimestampUnixNano(uint64(t.UnixNano()))
}
