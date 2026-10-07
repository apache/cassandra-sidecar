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

package org.apache.cassandra.sidecar.adapters.base;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RingProvider}
 */
class RingProviderTest
{
    private static final String LIVE_NODE = "127.0.0.1:7000";
    private static final String DEAD_NODE = "127.0.0.2:7000";
    private static final String UNCLASSIFIED_NODE = "127.0.0.3:7000";

    @Test
    void testStatusOfLiveAndDeadNodes()
    {
        RingProvider.Status status = new RingProvider.Status(List.of(LIVE_NODE), List.of(DEAD_NODE));

        assertThat(status.of(LIVE_NODE)).isEqualTo("Up");
        assertThat(status.of(DEAD_NODE)).isEqualTo("Down");
    }

    @Test
    void testStatusOfNodeNeitherLiveNorDeadIsUnknown()
    {
        RingProvider.Status status = new RingProvider.Status(List.of(LIVE_NODE), List.of(DEAD_NODE));

        // Clients parse the status into an enum, so it must not be "?"
        assertThat(status.of(UNCLASSIFIED_NODE)).isEqualTo("Unknown");
    }

    @Test
    void testStateOfNodeNeitherJoiningLeavingNorMovingIsNormal()
    {
        RingProvider.State state = new RingProvider.State(Collections.emptyList(),
                                                          Collections.emptyList(),
                                                          Collections.emptyList());

        assertThat(state.of(UNCLASSIFIED_NODE)).isEqualTo("Normal");
    }
}
