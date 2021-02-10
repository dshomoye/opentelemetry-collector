package kafkametricsreceiver

import (
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
)

const (
	testTopic          = "test_topic"
	testPartition      = 1
	testGroup          = "test_group"
	testConsumerClient = "test_consume_client"
	testBroker         = "test_broker"
)

type mockSaramaClient struct {
	mock.Mock
	sarama.Client

	brokers    func() []*sarama.Broker
	partitions func(string) ([]int32, error)
	getOffset  func(string, int32, int64) (int64, error)
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
	return s.brokers()
}

func (s *mockSaramaClient) Partitions(arg string) ([]int32, error) {
	return s.partitions(arg)
}

func (s *mockSaramaClient) GetOffset(arg1 string, arg2 int32, arg3 int64) (int64, error) {
	return s.getOffset(arg1, arg2, arg3)
}

func getMockClient() *mockSaramaClient {
	client := new(mockSaramaClient)
	r := sarama.NewBroker(testBroker)
	testBrokers := make([]*sarama.Broker, 1)
	testBrokers[0] = r
	client.brokers = func() []*sarama.Broker {
		return testBrokers
	}
	testTopics := make(map[string][]int32)
	testTopics[testTopic] = []int32{testPartition}
	client.partitions = func(s string) ([]int32, error) {
		return testTopics[s], nil
	}
	client.getOffset = func(string, int32, int64) (int64, error) {
		return 1, nil
	}
	return client
}

type mockClusterAdmin struct {
	mock.Mock
	sarama.ClusterAdmin

	listTopics               func() (map[string]sarama.TopicDetail, error)
	listConsumerGroups       func() (map[string]string, error)
	describeConsumerGroups   func([]string) ([]*sarama.GroupDescription, error)
	listConsumerGroupOffsets func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error)
}

func (s *mockClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	return s.listTopics()
}

func (s *mockClusterAdmin) ListConsumerGroups() (map[string]string, error) {
	return s.listConsumerGroups()
}

func (s *mockClusterAdmin) DescribeConsumerGroups(arg1 []string) ([]*sarama.GroupDescription, error) {
	return s.describeConsumerGroups(arg1)
}

func (s *mockClusterAdmin) ListConsumerGroupOffsets(arg1 string, arg2 map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	return s.listConsumerGroupOffsets(arg1, arg2)
}

func getMockClusterAdmin() *mockClusterAdmin {
	clusterAdmin := new(mockClusterAdmin)
	clusterAdmin.listConsumerGroups = func() (map[string]string, error) {
		r := make(map[string]string)
		r[testGroup] = testGroup
		return r, nil
	}
	clusterAdmin.listTopics = func() (map[string]sarama.TopicDetail, error) {
		td := make(map[string]sarama.TopicDetail)
		td[testTopic] = sarama.TopicDetail{}
		return td, nil
	}
	clusterAdmin.describeConsumerGroups = func(strings []string) ([]*sarama.GroupDescription, error) {
		desc := sarama.GroupMemberDescription{
			ClientId: testConsumerClient,
		}
		gmd := make(map[string]*sarama.GroupMemberDescription)
		gmd[testConsumerClient] = &desc
		d := sarama.GroupDescription{
			GroupId: testGroup,
			Members: gmd,
		}
		gd := make([]*sarama.GroupDescription, 1)
		gd[0] = &d
		return gd, nil
	}
	clusterAdmin.listConsumerGroupOffsets = func(s string, m map[string][]int32) (*sarama.OffsetFetchResponse, error) {
		blocks := make(map[string]map[int32]*sarama.OffsetFetchResponseBlock)
		topicBlocks := make(map[int32]*sarama.OffsetFetchResponseBlock)
		block := sarama.OffsetFetchResponseBlock{
			Offset: 1,
		}
		topicBlocks[testPartition] = &block
		blocks[testTopic] = topicBlocks
		offsetRes := sarama.OffsetFetchResponse{
			Blocks: blocks,
		}
		return &offsetRes, nil
	}
	return clusterAdmin
}
