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

import org.apache.cassandra.cdc.sidecar.SidecarCdc;
import org.apache.cassandra.cdc.sidecar.SidecarCdcStats;
import org.apache.cassandra.cdc.sidecar.SidecarStatePersister;

/**
 * Pairs a {@link SidecarCdc} consumer with its {@link SidecarStatePersister} so they are
 * always stopped together in the correct order: consumer first (blocking), then persister flush.
 */
class CdcConsumerEntry
{
    private final SidecarCdc consumer;
    private final SidecarStatePersister persister;
    private final SidecarCdcStats sidecarCdcStats;

    CdcConsumerEntry(SidecarCdc consumer, SidecarStatePersister persister, SidecarCdcStats sidecarCdcStats)
    {
        this.consumer = consumer;
        this.persister = persister;
        this.sidecarCdcStats = sidecarCdcStats;
    }

    SidecarCdc consumer()
    {
        return consumer;
    }

    SidecarStatePersister persister()
    {
        return persister;
    }

    void start()
    {
        persister.start();
        consumer.initSchema();
        consumer.start();
        sidecarCdcStats.captureCdcConsumerStarted();
    }

    void stop()
    {
        consumer.stop();           // blocking — waits for any active run() to complete
        persister.stop(true);      // flush buffered state to Cassandra, then cancel timer
        sidecarCdcStats.captureCdcConsumerStopped();
    }
}
