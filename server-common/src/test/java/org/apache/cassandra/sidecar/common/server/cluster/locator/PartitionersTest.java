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

package org.apache.cassandra.sidecar.common.server.cluster.locator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionersTest
{
    @Test
    void testCreatePartitionerFromString()
    {
        Partitioner random1 = Partitioners.from("org.apache.cassandra.dht.RandomPartitioner");
        Partitioner random2 = Partitioners.from("RandomPartitioner");
        assertThat(Partitioners.RANDOM).isSameAs(random1).isSameAs(random2);

        Partitioner murmur3a = Partitioners.from("org.apache.cassandra.dht.Murmur3Partitioner");
        Partitioner murmur3b = Partitioners.from("Murmur3Partitioner");
        assertThat(Partitioners.MURMUR3).isSameAs(murmur3a).isSameAs(murmur3b);
    }

    @Test
    void testCreatePartitionerFromInvalidString()
    {
        assertThatThrownBy(() -> Partitioners.from("foo"))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported partitioner");
    }
}
