/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package producer

import (
	"fmt"
	"github.com/rcrowley/go-metrics"
)

type ProducerMetrics interface {
	// NumOwnedTopicPartitions is a counter which value is the number of currently owned topic partitions by
	// enclosing Consumer.
	NumMetadataRefreshes(func(metrics.Counter))

	// Registry provides access to metrics registry for enclosing Consumer.
	Registry() metrics.Registry

	// Stop unregisters all metrics from the registry.
	Stop()
}

type KafkaProducerMetrics struct {
	registry metrics.Registry
}

// NewKafkaProducerMetrics creates new KafkaProducerMetrics for a given client ID.
func NewKafkaProducerMetrics(clientID string) *KafkaProducerMetrics {
	registry := metrics.NewPrefixedRegistry(fmt.Sprintf("%s.", clientID))

	return &KafkaProducerMetrics{
		registry: registry,
	}
}

// Registry provides access to metrics registry for enclosing Producer.
func (pm *KafkaProducerMetrics) Registry() metrics.Registry {
	return pm.registry
}

// Stop unregisters all metrics from the registry.
func (pm *KafkaProducerMetrics) Stop() {
	pm.registry.UnregisterAll()
}

var noOpProducerMetrics = new(noOpKafkaProducerMetrics)

type noOpKafkaProducerMetrics struct{}

func (*noOpKafkaProducerMetrics) Registry() metrics.Registry {
	panic("Registry() call on no op metrics")
}
func (*noOpKafkaProducerMetrics) Stop() {}
