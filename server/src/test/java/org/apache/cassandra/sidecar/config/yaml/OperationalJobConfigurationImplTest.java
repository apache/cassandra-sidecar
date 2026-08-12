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

package org.apache.cassandra.sidecar.config.yaml;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link OperationalJobConfigurationImpl}
 */
class OperationalJobConfigurationImplTest
{
    @Test
    void testDefaultTtl()
    {
        OperationalJobConfigurationImpl config = new OperationalJobConfigurationImpl();
        assertThat(config.tablesTtl()).isEqualTo(SecondBoundConfiguration.parse("90d"));
    }

    @Test
    void testTtlBelowMinimumThrows()
    {
        assertThatThrownBy(() -> OperationalJobConfigurationImpl.builder()
                                                                .tablesTtl(SecondBoundConfiguration.parse("13d"))
                                                                .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tablesTtl cannot be less than");
    }
}
