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
	"strconv"
	"time"

	"go.opentelemetry.io/collector/consumer/pdata"
)

const (
	partitionsName            = "kafka_topic_partitions"
	partitionsDescription     = "number of partitions for this topic"
	currentOffsetName         = "kafka_topic_current_offset"
	currentOffsetDescription  = "current offset of topic/partition"
	oldestOffsetName          = "kafka_topic_oldest_offset"
	oldestOffsetDescription   = "oldest offset of topic/partition"
	replicasName              = "kafka_topic_replicas"
	replicasDescription       = "number of replicas for topic/partition"
	replicasInSyncName        = "kafka_topic_replicas_in_sync"
	replicasInSyncDescription = "number of in-sync replicas for topic/partition"
	groupMembersName          = "kafka_consumer_group_members"
	groupMembersDescription   = "number of members in consumer group"
)

type topicMetrics struct {
	partitions     *pdata.Metric
	currentOffset  *pdata.Metric
	oldestOffset   *pdata.Metric
	replicas       *pdata.Metric
	replicasInSync *pdata.Metric
}

type consumerMetrics struct {
	groupMembers *pdata.Metric
}

func initializeTopicMetrics(metrics *pdata.MetricSlice) *topicMetrics {
	metrics.Resize(0)
	metrics.Resize(5)

	partitions := metrics.At(0)
	currentOffset := metrics.At(1)
	oldestOffset := metrics.At(2)
	replicas := metrics.At(3)
	replicasInSync := metrics.At(4)

	initializeMetric(&partitions, partitionsName, partitionsDescription)
	initializeMetric(&currentOffset, currentOffsetName, currentOffsetDescription)
	initializeMetric(&oldestOffset, oldestOffsetName, oldestOffsetDescription)
	initializeMetric(&replicas, replicasName, replicasDescription)
	initializeMetric(&replicasInSync, replicasInSyncName, replicasInSyncDescription)

	return &topicMetrics{
		partitions:     &partitions,
		currentOffset:  &currentOffset,
		oldestOffset:   &oldestOffset,
		replicas:       &replicas,
		replicasInSync: &replicasInSync,
	}
}

func initializeMetric(m *pdata.Metric, name string, description string) {
	m.SetName(name)
	m.SetDescription(description)
	m.SetDataType(pdata.MetricDataTypeIntGauge)
}

func addPartitionsToMetric(topic string, partitions int64, m *pdata.Metric, topicIdx int) {
	dp := m.IntGauge().DataPoints().At(topicIdx)
	dp.SetValue(partitions)
	dp.SetTimestamp(timeToUnixNano(time.Now()))
	dp.LabelsMap().InitFromMap(map[string]string{
		"topic": topic,
	})
}

func addPartitionDPToMetric(topic string, partition int32, value int64, m *pdata.Metric) {
	dpLen := m.IntGauge().DataPoints().Len()
	m.IntGauge().DataPoints().Resize(dpLen + 1)
	dp := m.IntGauge().DataPoints().At(dpLen)
	dp.SetValue(value)
	dp.SetTimestamp(timeToUnixNano(time.Now()))
	dp.LabelsMap().InitFromMap(map[string]string{
		"topic":     topic,
		"partition": int32ToStr(partition),
	})
}

func int32ToStr(i int32) string {
	return strconv.FormatInt(int64(i), 10)
}

func initializeConsumerMetrics(metrics *pdata.MetricSlice) *consumerMetrics {
	metrics.Resize(0)
	metrics.Resize(1)

	groupMembers := metrics.At(0)

	initializeMetric(&groupMembers, groupMembersName, groupMembersDescription)

	return &consumerMetrics{
		groupMembers: &groupMembers,
	}
}

func addGroupMembersToMetric(groupId string, members int64, m *pdata.Metric) {
	dpLen := m.IntGauge().DataPoints().Len()
	m.IntGauge().DataPoints().Resize(dpLen + 1)
	dp := m.IntGauge().DataPoints().At(dpLen)
	dp.SetValue(members)
	dp.SetTimestamp(timeToUnixNano(time.Now()))
	dp.LabelsMap().InitFromMap(map[string]string{
		"groupId": groupId,
	})
}
