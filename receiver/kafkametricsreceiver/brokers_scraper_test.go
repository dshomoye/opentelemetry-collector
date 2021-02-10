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
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

type mockSaramaClient struct {
	mock.Mock
	sarama.Client

	getBrokers func() []*sarama.Broker
}

func (s *mockSaramaClient) Closed() bool {
	s.Called()
	return false
}

func (s *mockSaramaClient) Close() error {
	s.Called()
	return nil
}

func (s *mockSaramaClient) Brokers() []*sarama.Broker {
	s.Called()
	return s.getBrokers()
}

func TestShutdown(t *testing.T) {
	client := new(mockSaramaClient)
	client.Mock.
		On("Close").Return(nil).
		On("Closed").Return(false)
	scraper := brokersScraper{
		client:       client,
		logger:       zap.NewNop(),
		saramaConfig: nil,
		config:       Config{},
	}
	_ = scraper.shutdown(context.Background())
	client.AssertExpectations(t)
}

func TestScrape_gets_brokers(t *testing.T) {
	client := new(mockSaramaClient)
	r := sarama.NewBroker("test")
	testBrokers := make([]*sarama.Broker, 1)
	testBrokers[0] = r
	client.getBrokers = func() []*sarama.Broker {
		return testBrokers
	}
	client.Mock.On("Brokers").Return(testBrokers)
	scraper := brokersScraper{
		client:       client,
		logger:       zap.NewNop(),
		saramaConfig: nil,
		config:       Config{},
	}
	ms, err := scraper.scrape(context.Background())
	client.AssertExpectations(t)
	assert.Nil(t, err)
	m := ms.At(0)
	assert.Equal(t, m.IntGauge().DataPoints().At(0).Value(), int64(1))
}
