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

package org.apache.cassandra.sidecar.common.data;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test {@link RingResponse}
 */
class RingResponseTest
{
    @Test
    void testRingResponseOrderByDc()
    {
        // add entries in reverse order
        RingResponse test = new RingResponse();
        for (int i = 9; i > 0; i--)
        {
            test.add(new RingEntry.Builder().datacenter("dc" + i).token("1").build());
        }

        assertThat(test).hasSize(9);
        // the entries are sorted by the datacenter name
        for (int i = 1; i <= 9; i++)
        {
            RingEntry entry = test.poll();
            assertThat(entry).isNotNull();
            assertThat(entry.datacenter()).isEqualTo("dc" + i);
        }
    }

    @Test
    void testRingResponseOrderByToken()
    {
        // add entries in reverse order
        RingResponse test = new RingResponse();
        for (int i = 9; i > 0; i--)
        {
            test.add(new RingEntry.Builder().datacenter("dc1").token(String.valueOf(i)).build());
        }

        assertThat(test).hasSize(9);
        // the entries are sorted by the datacenter name
        for (int i = 1; i <= 9; i++)
        {
            RingEntry entry = test.poll();
            assertThat(entry).isNotNull();
            assertThat(entry.token()).isEqualTo(String.valueOf(i));
        }
    }
}
