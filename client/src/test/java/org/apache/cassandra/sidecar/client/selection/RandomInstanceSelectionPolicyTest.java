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

package org.apache.cassandra.sidecar.client.selection;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstancesProvider;
import org.apache.cassandra.sidecar.client.SimpleSidecarInstancesProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the {@link RandomInstanceSelectionPolicy}
 */
class RandomInstanceSelectionPolicyTest
{
    List<SidecarInstance> mockInstanceList;

    @BeforeEach
    void setup()
    {
        mockInstanceList = Arrays.asList(mock(SidecarInstance.class), mock(SidecarInstance.class),
                                         mock(SidecarInstance.class), mock(SidecarInstance.class)
        );
    }

    @Test
    public void testIterator()
    {
        SidecarInstancesProvider provider = new SimpleSidecarInstancesProvider(mockInstanceList);
        InstanceSelectionPolicy instanceSelectionPolicy = new RandomInstanceSelectionPolicy(provider);
        Iterator<SidecarInstance> iterator = instanceSelectionPolicy.iterator();

        assertThat(iterator.hasNext()).isTrue().as("Expected to be true");
        assertThat(iterator.hasNext()).isTrue().as("Test idempotency of hasNext by running it again");
        assertThat(iterator.next()).isIn(mockInstanceList);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isIn(mockInstanceList);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isIn(mockInstanceList);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isIn(mockInstanceList);
        assertThat(iterator.hasNext()).isFalse().as("Expected to be false");
        assertThat(iterator.hasNext()).isFalse().as("Test idempotency of hasNext by running it again");;
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(iterator::next);
    }
}
