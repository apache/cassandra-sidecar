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

package org.apache.cassandra.sidecar.cdc;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.sidecar.SidecarCdc;
import org.apache.cassandra.cdc.sidecar.SidecarCdcStats;
import org.apache.cassandra.cdc.sidecar.SidecarStatePersister;
import org.mockito.InOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link CdcConsumerEntry}. */
public class CdcConsumerEntryTest
{
    @Test
    void startCallsPersisterBeforeConsumerThenCapturesConsumerStarted()
    {
        SidecarCdc consumer = mock(SidecarCdc.class);
        SidecarStatePersister persister = mock(SidecarStatePersister.class);
        SidecarCdcStats sidecarCdcStats = mock(SidecarCdcStats.class);
        CdcConsumerEntry entry = new CdcConsumerEntry(consumer, persister, sidecarCdcStats);

        entry.start();

        InOrder order = inOrder(persister, consumer, sidecarCdcStats);
        order.verify(persister).start();
        order.verify(consumer).initSchema();
        order.verify(consumer).start();
        order.verify(sidecarCdcStats).captureCdcConsumerStarted();
    }

    @Test
    void stopCallsConsumerBeforePersisterThenCapturesConsumerStopped()
    {
        SidecarCdc consumer = mock(SidecarCdc.class);
        SidecarStatePersister persister = mock(SidecarStatePersister.class);
        SidecarCdcStats sidecarCdcStats = mock(SidecarCdcStats.class);
        CdcConsumerEntry entry = new CdcConsumerEntry(consumer, persister, sidecarCdcStats);

        entry.stop();

        InOrder order = inOrder(consumer, persister, sidecarCdcStats);
        order.verify(consumer).stop();
        order.verify(persister).stop(true);
        order.verify(sidecarCdcStats).captureCdcConsumerStopped();
    }

    @Test
    void accessorsReturnConstructorArguments()
    {
        SidecarCdc consumer = mock(SidecarCdc.class);
        SidecarStatePersister persister = mock(SidecarStatePersister.class);
        SidecarCdcStats sidecarCdcStats = mock(SidecarCdcStats.class);
        CdcConsumerEntry entry = new CdcConsumerEntry(consumer, persister, sidecarCdcStats);

        assertThat(entry.consumer()).isSameAs(consumer);
        assertThat(entry.persister()).isSameAs(persister);
    }
}
