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

package org.apache.cassandra.sidecar.data;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.data.Name;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link RingRequest} object
 */
class RingRequestTest
{
    @Test
    void testEmptyConstructor()
    {
        RingRequest request = new RingRequest();
        assertThat(request).isNotNull();
        assertThat(request.keyspace()).isNull();
    }

    @Test
    void testConstructorWithParams()
    {
        RingRequest request = new RingRequest(new Name("valid_keyspace"));
        assertThat(request).isNotNull();
        assertThat(request.keyspace()).isNotNull();
        assertThat(request.keyspace().name()).isEqualTo("valid_keyspace");
    }

    @Test
    void testToString()
    {
        RingRequest request1 = new RingRequest(new Name("valid_keyspace"));
        RingRequest request2 = new RingRequest(new Name("ks2"));
        assertThat(request1).hasToString("RingRequest{keyspace='valid_keyspace'}");
        assertThat(request2).hasToString("RingRequest{keyspace='ks2'}");
    }

    @Test
    void testEquals()
    {
        RingRequest request1 = new RingRequest(new Name("ks"));
        RingRequest request2 = new RingRequest(new Name("ks"));
        RingRequest request3 = new RingRequest(new Name("ks5"));
        assertThat(request1).isEqualTo(request2);
        assertThat(request1).isNotSameAs(request2);
        assertThat(request1).isNotEqualTo(request3);
    }

    @Test
    void testHashCode()
    {
        RingRequest request1 = new RingRequest(new Name("ks"));
        RingRequest request2 = new RingRequest(new Name("ks"));
        RingRequest request3 = new RingRequest(new Name("ks5"));
        assertThat(request1).hasSameHashCodeAs(request2);
        assertThat(request1).doesNotHaveSameHashCodeAs(request3);
    }
}
