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

package org.apache.cassandra.sidecar.tasks;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.codecs.DcLocalTopologyChangeEventCodec;

import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.PeriodicTaskConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.coordination.TokenRingProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Periodic task that monitors Cassandra cluster topology changes and publishes events when detected.
 */
@Singleton
public class ClusterTopologyMonitor implements PeriodicTask
{
    /**
     * Event types published when cluster topology changes are detected.
     */
    public enum ClusterTopologyEventType
    {
        ON_DC_TOPOLOGY_CHANGE;

        /**
         * Returns the event bus address for this event type.
         */
        public String address()
        {
            return ClusterTopologyMonitor.class.getName() + "." + name();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTopologyMonitor.class);
    private final PeriodicTaskConfiguration configuration;
    private final Vertx vertx;
    private final TokenRingProvider tokenRingProvider;

    private final ConcurrentHashMap<String, Map<String, List<TokenRange>>> perDcRanges = new ConcurrentHashMap<>(4);

    /**
     * Event data containing datacenter-local topology change information.
     */
    public static class DcLocalTopologyChangeEvent
    {
        public final String dc;
        @Nullable
        public final Map<String, List<TokenRange>> prev;
        @NotNull
        public final Map<String, List<TokenRange>> curr;

        /**
         * Creates a new datacenter topology change event.
         */
        public DcLocalTopologyChangeEvent(String dc,
                                          @Nullable Map<String, List<TokenRange>> prev,
                                          @NotNull Map<String, List<TokenRange>> curr)
        {
            this.dc = dc;
            this.prev = prev;
            this.curr = curr;
        }

        /**
         * Factory method to create a topology change event with instance ID keyed maps.
         */
        public static DcLocalTopologyChangeEvent of(String dc,
                                                    @Nullable Map<String, List<TokenRange>> prev,
                                                    @NotNull Map<String, List<TokenRange>> curr)
        {
            return new DcLocalTopologyChangeEvent(dc, keyByInstanceId(prev), keyByInstanceId(curr));
        }

        private static Map<String, List<TokenRange>> keyByInstanceId(@Nullable Map<String, List<TokenRange>> map)
        {
            return map == null ? null : map.entrySet().stream()
                                           .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
                                                                                 Map.Entry::getValue));
        }

        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            DcLocalTopologyChangeEvent that = (DcLocalTopologyChangeEvent) o;
            return Objects.equals(dc, that.dc) &&
                   Objects.equals(prev, that.prev) &&
                   curr.equals(that.curr);
        }

        public int hashCode()
        {
            return Objects.hash(dc, prev, curr);
        }
    }

    /**
     * Creates a cluster topology monitor with periodic task scheduling.
     */
    @Inject
    public ClusterTopologyMonitor(Vertx vertx,
                                  TokenRingProvider tokenRingProvider,
                                  PeriodicTaskExecutor periodicTaskExecutor,
                                  SidecarConfiguration configuration)
    {
        this(vertx, tokenRingProvider, configuration);
        LOGGER.info("Starting Cluster Topology Monitor");
        periodicTaskExecutor.schedule(this);
        LOGGER.info("Cluster Topology Monitor started");
    }

    /**
     * Creates a cluster topology monitor without scheduling.
     */
    public ClusterTopologyMonitor(Vertx vertx, TokenRingProvider tokenRingProvider, SidecarConfiguration configuration)
    {
        this.configuration = configuration.clusterTopologyMonitorConfiguration();
        this.vertx = vertx;
        this.tokenRingProvider = tokenRingProvider;
        try
        {
            vertx.eventBus().registerDefaultCodec(DcLocalTopologyChangeEvent.class, DcLocalTopologyChangeEventCodec.INSTANCE);
        }
        catch (IllegalStateException ise)
        {
            if (!ise.getMessage().contains("Already a default codec registered"))
            {
                throw ise;
            }
        }
    }

    /**
     * Returns the initial delay before the first execution.
     */
    @Override
    public DurationSpec initialDelay()
    {
        return configuration.initialDelay();
    }

    /**
     * Returns the delay between topology refresh cycles.
     */
    @Override
    public DurationSpec delay()
    {
        return configuration.executeInterval();
    }

    /**
     * Executes the topology monitoring task.
     */
    @Override
    public void execute(Promise<Void> promise)
    {
        try
        {
            refresh();
        }
        catch (Exception e)
        {
            LOGGER.error("Unexpected error in ClusterTopologyMonitor", e);
        }
        finally
        {
            promise.complete();
        }
    }

    /**
     * Refreshes topology information for all datacenters and publishes change events.
     */
    protected void refresh()
    {
        for (String dc : tokenRingProvider.dcs())
        {
            Map<String, List<TokenRange>> prev = perDcRanges.get(dc);

            Map<String, List<TokenRange>> curr = tokenRingProvider.getPrimaryTokenRanges(dc);
            if (!curr.equals(prev) && changeInInstanceTopology(prev, curr) && update(dc, prev, curr))
            {
                LOGGER.info("Publishing ON_DC_TOPOLOGY_CHANGE event");
                vertx.eventBus().publish(ClusterTopologyEventType.ON_DC_TOPOLOGY_CHANGE.address(),
                                         DcLocalTopologyChangeEvent.of(dc, prev, curr));
            }
        }
    }

    protected boolean update(String dc,
                             @Nullable Map<String, List<TokenRange>> prev,
                             Map<String, List<TokenRange>> curr)
    {
        if (prev == null)
        {
            return perDcRanges.putIfAbsent(dc, curr) == null;
        }
        return perDcRanges.replace(dc, prev, curr);
    }

    protected static boolean changeInInstanceTopology(@Nullable Map<String, List<TokenRange>> prev,
                                                      @NotNull Map<String, List<TokenRange>> curr)
    {
        if (prev == null)
        {
            LOGGER.info("Bootstrapping cluster topology curr={}", curr.size());
            return true;
        }

        boolean result = false;
        if (prev.size() != curr.size())
        {
            LOGGER.info("Cluster topology change detected: change in instance count prev={} curr={}", prev.size(), curr.size());
            result = true;
        }

        for (Map.Entry<String, List<TokenRange>> entry : curr.entrySet())
        {
            final String instanceId = entry.getKey();
            @Nullable final List<TokenRange> prevRanges = prev.get(instanceId);
            final List<TokenRange> currRanges = entry.getValue();
            if (prevRanges == null)
            {
                LOGGER.info("Cluster topology change detected: new instance added instanceId={} prev='{}' curr='{}'", instanceId, prev, curr);
                result = true;
            }
            else if (changeInInstanceTopology(instanceId, prevRanges, currRanges))
            {
                result = true;
            }
        }

        for (Map.Entry<String, List<TokenRange>> entry : prev.entrySet())
        {
            final String instanceId = entry.getKey();
            if (!curr.containsKey(instanceId))
            {
                LOGGER.info("Cluster topology change detected: existing instance removed instanceId={} prev='{}' curr='{}'", instanceId, prev, curr);
                result = true;
            }
        }

        return result;
    }

    protected static boolean changeInInstanceTopology(String instanceId, List<TokenRange> prev, List<TokenRange> curr)
    {
        if (prev.size() != curr.size())
        {
            LOGGER.info("Change in instance tokens instanceId={} prev='{}' curr='{}'", instanceId, prev, curr);
            return true;
        }

        prev = new ArrayList<>(prev);
        prev.sort(Comparator.comparing(t -> t.range.lowerEndpoint()));
        curr = new ArrayList<>(curr);
        curr.sort(Comparator.comparing(t -> t.range.lowerEndpoint()));

        boolean result = false;
        for (int i = 0; i < prev.size(); i++)
        {
            final TokenRange prevRange = prev.get(i);
            final TokenRange currRange = curr.get(i);
            if (!prevRange.range.lowerEndpoint().equals(currRange.range.lowerEndpoint()))
            {
                LOGGER.info("Change in instance lower token instanceId={} prev={} curr={}", instanceId, prevRange.range.lowerEndpoint(), currRange.range.lowerEndpoint());
                result = true;
            }
            if (!prevRange.range.upperEndpoint().equals(currRange.range.upperEndpoint()))
            {
                LOGGER.info("Change in instance upper token instanceId={} prev={} curr={}", instanceId, prevRange.range.upperEndpoint(), currRange.range.upperEndpoint());
                result = true;
            }
        }

        return result;
    }
}
