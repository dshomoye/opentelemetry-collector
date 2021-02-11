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
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestBrokerShutdown(t *testing.T) {
	client := getMockClient()
	client.Mock.
		On("Close").Return(nil).
		On("Closed").Return(false)
	scraper := brokerScraper{
		client: client,
		logger: zap.NewNop(),
		config: Config{},
	}
	_ = scraper.shutdown(context.Background())
	client.AssertExpectations(t)
}

func TestBrokerScraper_Name(t *testing.T) {
	s := brokerScraper{}
	assert.Equal(t, s.Name(), "brokers")
}

func TestBrokerScraper_gets_brokers(t *testing.T) {
	client := getMockClient()
	r := sarama.NewBroker(testBroker)
	testBrokers := make([]*sarama.Broker, 1)
	testBrokers[0] = r
	client.Mock.On("Brokers").Return(testBrokers)
	scraper := brokerScraper{
		client: client,
		logger: zap.NewNop(),
		config: Config{},
	}
	ms, err := scraper.scrape(context.Background())
	client.AssertExpectations(t)
	assert.Nil(t, err)
	m := ms.At(0)
	assert.Equal(t, m.IntGauge().DataPoints().At(0).Value(), int64(len(testBrokers)))
}

func TestBrokersScraper_createBrokerScraper(t *testing.T) {
	sc := sarama.NewConfig()
	newSaramaClient = mockNewSaramaClient
	ms, err := createBrokerScraper(context.Background(), Config{}, sc, zap.NewNop())
	assert.Nil(t, err)
	assert.NotNil(t, ms)
}
