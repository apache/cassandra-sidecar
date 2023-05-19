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

package org.apache.cassandra.sidecar.client;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * Abstract class that provides a set of base unit tests for the {@link SidecarInstance} interface.
 */
public abstract class SidecarInstanceTest
{
    protected abstract SidecarInstance newInstance(String hostname, int port);

    @ParameterizedTest
    @ValueSource(ints = { -1, 0, 65536, 100_000 })
    void failsWithInvalidPortNumber(int port)
    {
        assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> newInstance(null, port))
        .withMessageContaining("Invalid port number for the Sidecar service: " + port);
    }

    @Test
    void failsWithNullHostname()
    {
        assertThatNullPointerException()
        .isThrownBy(() -> newInstance(null, 8080))
        .withMessageContaining("The Sidecar hostname must be non-null");
    }

    @Test
    void testConstructorWithValidParameters()
    {
        SidecarInstance instance1 = newInstance("localhost", 8080);
        assertThat(instance1).isNotNull();
        assertThat(instance1.port()).isEqualTo(8080);
        assertThat(instance1.hostname()).isEqualTo("localhost");

        SidecarInstance instance2 = newInstance("127.0.0.1", 1234);
        assertThat(instance2).isNotNull();
        assertThat(instance2.port()).isEqualTo(1234);
        assertThat(instance2.hostname()).isEqualTo("127.0.0.1");
    }

    @Test
    void testEquality()
    {
        SidecarInstance instance1 = newInstance("localhost", 8080);
        SidecarInstance instance2 = newInstance("localhost", 8080);
        SidecarInstance instance3 = instance1;

        assertThat(instance1).isEqualTo(instance3);
        assertThat(instance1).isNotEqualTo(null);
        assertThat(instance1).isNotEqualTo("localhost:8080");
        assertThat(instance1).isEqualTo(instance2);
    }

    @Test
    void testHashCode()
    {
        SidecarInstance instance1 = newInstance("localhost", 8080);
        SidecarInstance instance2 = newInstance("localhost", 8080);
        SidecarInstance instance3 = newInstance("localhost", 80);

        assertThat(instance1.hashCode()).isEqualTo(instance2.hashCode());
        assertThat(instance1.hashCode()).isNotEqualTo(instance3.hashCode());
    }
}
