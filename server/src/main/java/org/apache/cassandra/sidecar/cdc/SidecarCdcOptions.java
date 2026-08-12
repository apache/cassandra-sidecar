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

import java.time.Duration;
import java.util.Map;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.ReplicationFactor;

/**
 * Specific sidecar CDC options, consumed by the CDC read path ({@code SidecarCdc}).
 *
 * <p>Delegates the throughput/backpressure-related knobs to {@link CdcConfig} so that they are
 * live-tunable from the DB-backed "configs" table (via {@link org.apache.cassandra.sidecar.tasks.CdcConfigRefresherNotifierTask})
 * without a Sidecar restart, instead of silently falling back to the {@link CdcOptions} interface
 * defaults baked into the cassandra-analytics library.
 */
public class SidecarCdcOptions implements CdcOptions
{
    private final InstanceMetadataFetcher instanceMetadataFetcher;
    private final CdcConfig conf;

    public SidecarCdcOptions(InstanceMetadataFetcher instanceMetadataFetcher, CdcConfig conf)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.conf = conf;
    }


    public ReplicationFactor replicationFactor(String keyspace)
    {

        Map<String, String> replication = instanceMetadataFetcher
                                          .callOnFirstAvailableInstance(instance-> instance.delegate().metadata().getKeyspace(keyspace).getReplication());
        return new ReplicationFactor(replication);
    }

    public String dc()
    {
        return instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings().datacenter());
    }

    @Override
    public CassandraVersion version()
    {
        String releaseVersion = instanceMetadataFetcher.callOnFirstAvailableInstance(
                instance -> instance.delegate().nodeSettings().releaseVersion());
        return CassandraVersion.fromVersion(releaseVersion).orElse(CassandraVersion.FOURZERO);
    }

    /**
     * Add an optional delay between micro-batches, to slow CDC down if it is overwhelming Cassandra
     * or the downstream Kafka publish stage. Backed by {@code CdcConfig.minDelayBetweenMicroBatches()}
     * so it can be lowered/raised live via the "configs" table, e.g. to accelerate cdc_raw drain
     * during a backlog without a restart.
     */
    @Override
    public Duration minimumDelayBetweenMicroBatches()
    {
        return Duration.ofMillis(conf.minDelayBetweenMicroBatches().toMillis());
    }

    /**
     * Throttles how many commit logs are read per epoch per instance. Backed by
     * {@code CdcConfig.maxCommitLogsPerInstance()} so it can be raised live to catch up faster
     * on a backlog, or lowered to bound per-batch memory/duration during burst load.
     */
    @Override
    public int maxCommitLogsPerInstance()
    {
        return conf.maxCommitLogsPerInstance();
    }

    /**
     * Maximum number of late/un-acked mutation digests held in the CDC watermarker state. Backed
     * by {@code CdcConfig.maxWatermarkerSize()}.
     *
     * <p><b>Caution:</b> {@code CdcState.ReplicaCountSerializer} currently serializes this map's
     * size with {@code writeShort}/{@code readShort} (signed 16-bit, max 32767). Do not configure
     * this above 32767 until that serializer is widened to an int, or persisted CDC state can
     * silently corrupt (observed as a permanent restart-crash-loop in production).
     */
    @Override
    public int maxCdcStateSize()
    {
        return conf.maxWatermarkerSize();
    }

    /**
     * Maximum age of mutations retained in the CDC watermarker before being purged (and counted via
     * {@code droppedExpiredMutations}). Backed by {@code CdcConfig.watermarkWindow()} -- previously
     * this value was entirely unreachable: {@code watermarkWindow()} was read from the DB-backed
     * config but never consulted by the CDC engine, which instead silently used the 1-hour
     * {@link CdcOptions#maximumAge()} interface default regardless of what operators configured.
     */
    @Override
    public Duration maximumAge()
    {
        return Duration.ofSeconds(conf.watermarkWindow().toSeconds());
    }
}
