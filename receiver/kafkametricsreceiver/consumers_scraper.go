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
	"go.opentelemetry.io/collector/receiver/scraperhelper"
	"regexp"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer/pdata"
)

type consumersScraper struct {
	client       sarama.Client
	logger       *zap.Logger
	groupFilter  *regexp.Regexp
	config       Config
	saramaConfig *sarama.Config
	clusterAdmin sarama.ClusterAdmin
}

func (s *consumersScraper) Name() string {
	return "consumers"
}

func (s *consumersScraper) shutdown(_ context.Context) error {
	if !s.client.Closed() {
		return s.client.Close()
	}
	return nil
}

func (s *consumersScraper) scrape(context.Context) (pdata.MetricSlice, error) {
	metrics := pdata.NewMetricSlice()
	allMetrics := initializeConsumerMetrics(&metrics)
	groupMembersMetric := allMetrics.groupMembers
	cgs, err := s.clusterAdmin.ListConsumerGroups()
	if err != nil {
		return metrics, err
	}
	var matchedGrpIds []string
	for grpID := range cgs {
		if s.groupFilter.MatchString(grpID) {
			matchedGrpIds = append(matchedGrpIds, grpID)
		}
	}
	consumerGroups, err := s.clusterAdmin.DescribeConsumerGroups(matchedGrpIds)
	if err != nil {
		return metrics, err
	}

	groupMembersMetric.IntGauge().DataPoints().Resize(len(consumerGroups))
	for _, group := range consumerGroups {
		addGroupMembersToMetric(group.GroupId, int64(len(group.Members)), groupMembersMetric)
	}

	return metrics, nil
}

func createConsumersScraper(_ context.Context, config Config, saramaConfig *sarama.Config, logger *zap.Logger) (scraperhelper.MetricsScraper, error) {
	groupFilter := regexp.MustCompile(config.GroupMatch)
	client, err := sarama.NewClient(config.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}
	clusterAdmin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, err
	}
	s := consumersScraper{
		client:       client,
		logger:       logger,
		groupFilter:  groupFilter,
		clusterAdmin: clusterAdmin,
	}
	return scraperhelper.NewMetricsScraper(
		s.Name(),
		s.scrape,
		scraperhelper.WithShutdown(s.shutdown),
	), nil
}
