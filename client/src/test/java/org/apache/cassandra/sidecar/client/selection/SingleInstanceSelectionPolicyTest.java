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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.client.SidecarInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

class SingleInstanceSelectionPolicyTest
{
    SidecarInstance mockSidecarInstance;

    @BeforeEach
    void setup()
    {
        mockSidecarInstance = mock(SidecarInstance.class);
    }

    @Test
    void testIterator()
    {
        InstanceSelectionPolicy instanceSelectionPolicy = new SingleInstanceSelectionPolicy(mockSidecarInstance);
        Iterator<SidecarInstance> iterator = instanceSelectionPolicy.iterator();

        assertThat(iterator.hasNext()).isTrue().as("Expected to be true");
        assertThat(iterator.hasNext()).isTrue().as("Test idempotency of hasNext by running it again");
        assertThat(iterator.next()).isSameAs(mockSidecarInstance);
        assertThat(iterator.hasNext()).isFalse().as("Expected to be false");
        assertThat(iterator.hasNext()).isFalse().as("Test idempotency of hasNext by running it again");
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(iterator::next);
    }
}
