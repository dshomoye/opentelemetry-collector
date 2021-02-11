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
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"regexp"
	"testing"
)

func TestConsumerShutdown(t *testing.T) {
	client := getMockClient()
	client.Mock.
		On("Close").Return(nil).
		On("Closed").Return(false)
	scraper := consumerScraper{
		client: client,
	}
	_ = scraper.shutdown(context.Background())
	client.AssertExpectations(t)
}

func TestConsumerScraper_Name(t *testing.T) {
	s := consumerScraper{}
	assert.Equal(t, s.Name(), "consumers")
}

func TestConsumerScraper_scrape_gets_all_metrics(t *testing.T) {
	client := getMockClient()
	clusterAdmin := getMockClusterAdmin()
	config := createDefaultConfig().(*Config)
	match := regexp.MustCompile(config.TopicMatch)
	scraper := consumerScraper{
		client:       client,
		logger:       zap.NewNop(),
		groupFilter:  match,
		topicFilter:  match,
		clusterAdmin: clusterAdmin,
	}
	ms, err := scraper.scrape(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, ms.Len(), 5)
	gm := ms.At(0)
	assert.Equal(t, gm.Name(), groupMembersName)
	assert.Equal(t, gm.IntGauge().DataPoints().At(0).Value(), int64(1), "group members must match test value")
	co := ms.At(1)
	assert.Equal(t, co.Name(), consumerOffsetName)
	assert.Equal(t, co.IntGauge().DataPoints().At(0).Value(), int64(1), "consumer offset must match test value")
	cl := ms.At(2)
	assert.Equal(t, cl.Name(), consumerLagName)
	assert.Equal(t, cl.IntGauge().DataPoints().At(0).Value(), int64(0), "consumer lag must match test value")
	ls := ms.At(3)
	assert.Equal(t, ls.Name(), lagSumName)
	assert.Equal(t, ls.IntGauge().DataPoints().At(0).Value(), int64(0), "lag sum must match test value")
	os := ms.At(4)
	assert.Equal(t, os.Name(), offsetSumName)
	assert.Equal(t, os.IntGauge().DataPoints().At(0).Value(), int64(1), "offset sum must match test value")
}

func TestConsumerScraper_createConsumerScraper(t *testing.T) {
	sc := sarama.NewConfig()
	newSaramaClient = mockNewSaramaClient
	newClusterAdmin = mockNewClusterAdmin
	ms, err := createConsumerScraper(context.Background(), Config{}, sc, zap.NewNop())
	assert.Nil(t, err)
	assert.NotNil(t, ms)
}
