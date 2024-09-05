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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the new metric type added {@link DeltaGauge}
 */
class DeltaGaugeTest
{
    @Test
    void testCumulativeCountUpdated()
    {
        DeltaGauge deltaGauge = new DeltaGauge();

        deltaGauge.update(3);
        deltaGauge.update(7);
        assertThat(deltaGauge.getValue()).isEqualTo(10);
        assertThat(deltaGauge.getValue()).isEqualTo(0);
        deltaGauge.update(2);
        assertThat(deltaGauge.getValue()).isEqualTo(2);
        assertThat(deltaGauge.getValue()).isEqualTo(0);
        deltaGauge.update(3);
        deltaGauge.update(1);
        deltaGauge.update(0);
        assertThat(deltaGauge.getValue()).isEqualTo(4);
        assertThat(deltaGauge.getValue()).isEqualTo(0);
    }
}
