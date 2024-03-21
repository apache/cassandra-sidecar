/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests for {@link NamedMetric}
 */
public class NamedMetricTest
{
    private Logger logger = LoggerFactory.getLogger(NamedMetricTest.class);
    private MetricRegistry registry = registry();

    @AfterEach
    void clear()
    {
        registry.removeMatching((name, metric) -> true);
    }

    @Test
    void printAllMetricNames()
    {
        registry.getMetrics().forEach((name, metric) -> logger.info("Metric name: {}, value: {}", name, metric));
    }

    @Test
    void testMetricTypeRetrieved()
    {
        NamedMetric namedMetric = NamedMetric.builder(name -> registry.gauge(name))
                                             .withName("testGauge")
                                             .withDomain("testDomain")
                                             .addTag("key", "value")
                                             .build();
        assertThat(namedMetric.metric).isInstanceOf(Gauge.class);
        assertThat(namedMetric.canonicalName).isEqualTo("testDomain.key=value.testGauge");
    }
}
