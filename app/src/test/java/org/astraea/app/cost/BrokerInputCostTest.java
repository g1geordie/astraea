/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.broker.BrokerTopicMetricsResult;
import org.astraea.app.metrics.collector.BeanCollector;
import org.astraea.app.metrics.collector.Receiver;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class BrokerInputCostTest extends RequireBrokerCluster {
  @Test
  void testTScoreCost() {
    var brokerInputCost = new BrokerInputCost();
    var scores =
        brokerInputCost
            .brokerCost(ClusterInfo.EMPTY, clusterBean(50000L, 20000L, 5000L))
            .normalize(Normalizer.TScore())
            .value();
    Assertions.assertEquals(0.63, scores.get(1));
    Assertions.assertEquals(0.47, scores.get(2));
    Assertions.assertEquals(0.39, scores.get(3));

    scores =
        brokerInputCost
            .brokerCost(ClusterInfo.EMPTY, clusterBean(55555L, 25352L, 25000L))
            .normalize(Normalizer.TScore())
            .value();
    Assertions.assertEquals(0.64, scores.get(1));
    Assertions.assertEquals(0.43, scores.get(2));
    Assertions.assertEquals(0.43, scores.get(3));
  }

  @Test
  void testNoNormalize() {
    var brokerInputCost = new BrokerInputCost();
    var scores =
        brokerInputCost.brokerCost(ClusterInfo.EMPTY, clusterBean(10000L, 20000L, 5000L)).value();
    Assertions.assertEquals(10000.0, scores.get(1));
    Assertions.assertEquals(20000.0, scores.get(2));
    Assertions.assertEquals(5000.0, scores.get(3));
  }

  @Test
  void testFetcher() {
    try (Receiver receiver =
        BeanCollector.builder()
            .build()
            .register()
            .host(jmxServiceURL().getHost())
            .port(jmxServiceURL().getPort())
            .fetcher(new BrokerInputCost().fetcher().get())
            .build()) {
      Assertions.assertFalse(receiver.current().isEmpty());

      // Test the fetched object's type, and its metric name.
      Assertions.assertTrue(
          receiver.current().stream()
              .allMatch(
                  o ->
                      (o instanceof BrokerTopicMetricsResult)
                          && (KafkaMetrics.BrokerTopic.BytesInPerSec.metricName()
                              .equals(o.beanObject().properties().get("name")))));

      // Test the fetched object's value.
      Assertions.assertTrue(
          receiver.current().stream()
              .map(o -> (BrokerTopicMetricsResult) o)
              .allMatch(result -> result.count() == 0));
    }
  }

  private ClusterBean clusterBean(long in1, long in2, long in3) {
    var BytesInPerSec1 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), in1);
    var BytesInPerSec2 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), in2);
    var BytesInPerSec3 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), in3);

    Collection<HasBeanObject> broker1 = List.of(BytesInPerSec1);
    Collection<HasBeanObject> broker2 = List.of(BytesInPerSec2);
    Collection<HasBeanObject> broker3 = List.of(BytesInPerSec3);
    return ClusterBean.of(Map.of(1, broker1, 2, broker2, 3, broker3));
  }

  private BrokerTopicMetricsResult mockResult(String name, long count) {
    var result = Mockito.mock(BrokerTopicMetricsResult.class);
    var bean = Mockito.mock(BeanObject.class);
    Mockito.when(result.beanObject()).thenReturn(bean);
    Mockito.when(bean.properties()).thenReturn(Map.of("name", name));
    Mockito.when(result.count()).thenReturn(count);
    return result;
  }
}
