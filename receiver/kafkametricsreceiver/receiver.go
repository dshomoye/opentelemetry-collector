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
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/kafkaexporter"
	"go.opentelemetry.io/collector/receiver/scraperhelper"
)

var (
	allScrapers = map[string]func(context.Context, Config, sarama.Client, *zap.Logger) scraperhelper.MetricsScraper{
		"topics":  createTopicsScraper,
		"brokers": createBrokersScraper,
	}
)

func newMetricsReceiver(
	ctx context.Context,
	config Config, params component.ReceiverCreateParams,
	consumer consumer.MetricsConsumer,
) (component.MetricsReceiver, error) {
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
	client, err := sarama.NewClient(config.Brokers, c)
	if err != nil {
		return nil, err
	}
	scraperControllerOptions := make([]scraperhelper.ScraperControllerOption, 0, len(config.Scrapers))
	for _, scraper := range config.Scrapers {
		if s, ok := allScrapers[scraper]; ok {
			s := s(ctx, config, client, params.Logger)
			scraperControllerOptions = append(scraperControllerOptions, scraperhelper.AddMetricsScraper(s))
			continue
		}
		return nil, fmt.Errorf("no scraper found for key: %s ", scraper)
	}

	return scraperhelper.NewScraperControllerReceiver(
		&config.ScraperControllerSettings,
		params.Logger,
		consumer,
		scraperControllerOptions...,
	)
}

func timeToUnixNano(t time.Time) pdata.TimestampUnixNano {
	return pdata.TimestampUnixNano(uint64(t.UnixNano()))
}
