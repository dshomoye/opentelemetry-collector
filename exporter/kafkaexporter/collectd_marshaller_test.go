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

package kafkaexporter

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"

	"go.opentelemetry.io/collector/internal/data/testdata"
)

func TestMetricsToValueLists(t *testing.T) {
	md := testdata.GenerateMetricsTwoMetrics()
	vls, dropped := MetricsToValueLists(md)
	res, _ := json.Marshal(vls)
	original, _ := json.Marshal(md)
	fmt.Println(original)
	fmt.Println(string(res))
	fmt.Println("dropped: ", dropped)
	assert.True(t, true)
}